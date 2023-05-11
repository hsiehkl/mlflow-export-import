"""
Imports a run from a directory.
"""

import os
import tempfile
import click
import base64

import mlflow
from mlflow.entities.lifecycle_stage import LifecycleStage
from mlflow.entities import RunStatus
from mlflow.utils.mlflow_tags import MLFLOW_PARENT_RUN_ID

from mlflow_export_import.common.click_options import (
    opt_input_dir,
    opt_import_source_tags,
    opt_experiment_name,
    opt_use_src_user_id,
    opt_dst_notebook_dir
)
from mlflow_export_import.common import utils, mlflow_utils, io_utils
from mlflow_export_import.common.filesystem import EmptyExperimentPathException, mk_local_path
from mlflow_export_import.common.find_artifacts import find_artifacts
from mlflow_export_import.common import filesystem as _filesystem
from mlflow_export_import.common import MlflowExportImportException
from mlflow_export_import.client.http_client import DatabricksHttpClient
from mlflow_export_import.run import run_data_importer

_logger = utils.getLogger(__name__)


def import_run(
        experiment_name,
        input_dir,
        dst_notebook_dir = None,
        import_source_tags = False,
        use_src_user_id = False,
        dst_notebook_dir_add_run_id = False,
        mlmodel_fix = True,
        mlflow_client = None
    ):
    """
    Imports a run into the specified experiment.
    :param experiment_name: Experiment name.
    :param input_dir: Source input directory that contains the exported run.
    :param dst_notebook_dir: Databricks destination workpsace directory for notebook.
    :param import_source_tags: Import source information for MLFlow objects and create tags in destination object.
    :param mlmodel_fix: Add correct run ID in destination MLmodel artifact.
                        Can be expensive for deeply nested artifacts.
    :param use_src_user_id: Set the destination user ID to the source user ID.
                            Source user ID is ignored when importing into
                            Databricks since setting it is not allowed.
    :param dst_notebook_dir: Databricks destination workspace directory for notebook import.
    :param dst_notebook_dir_add_run_id: Add the run ID to the destination notebook directory.
    :param mlflow_client: MLflow client.
    :return: The run and its parent run ID if the run is a nested run.
    """
    importer = RunImporter(
        import_source_tags = import_source_tags,
        use_src_user_id = use_src_user_id,
        dst_notebook_dir_add_run_id = dst_notebook_dir_add_run_id,
        mlmodel_fix = mlmodel_fix,
        mlflow_client = mlflow_client
    )
    return importer.import_run(
        experiment_name = experiment_name,
        input_dir = input_dir,
        dst_notebook_dir = dst_notebook_dir
    )


class RunImporter():
    def __init__(self, 
            mlflow_client = None,
            import_source_tags = False,
            mlmodel_fix = True,
            use_src_user_id = False,
            dst_notebook_dir_add_run_id = False
        ):
        """ 
        :param mlflow_client: MLflow client.
        :param import_source_tags: Import source information for MLFlow objects and create tags in destination object.
        :param mlmodel_fix: Add correct run ID in destination MLmodel artifact. 
                            Can be expensive for deeply nested artifacts.
        :param use_src_user_id: Set the destination user ID to the source user ID. 
                                Source user ID is ignored when importing into 
                                Databricks since setting it is not allowed.
        :param dst_notebook_dir: Databricks destination workspace directory for notebook import.
        :param dst_notebook_dir_add_run_id: Add the run ID to the destination notebook directory.
        """

        self.mlflow_client = mlflow_client or mlflow.client.MlflowClient()
        self.mlmodel_fix = mlmodel_fix
        self.use_src_user_id = use_src_user_id
        self.in_databricks = "DATABRICKS_RUNTIME_VERSION" in os.environ
        self.dst_notebook_dir_add_run_id = dst_notebook_dir_add_run_id
        self.dbx_client = DatabricksHttpClient()
        self.import_source_tags = import_source_tags
        _logger.debug(f"in_databricks: {self.in_databricks}")
        _logger.debug(f"importing_into_databricks: {utils.importing_into_databricks()}")


    def import_run(self, 
            experiment_name,
            input_dir,
            dst_notebook_dir = None
        ):
        """ 
        Imports a run into the specified experiment.
        :param experiment_name: Experiment name.
        :param input_dir: Source input directory that contains the exported run.
        :param dst_notebook_dir: Databricks destination workpsace directory for notebook.
        :return: The run and its parent run ID if the run is a nested run.
        """
        _logger.info(f"Importing run from '{input_dir}'")
        res = self._import_run(experiment_name, input_dir, dst_notebook_dir)
        _logger.info(f"Imported run into '{experiment_name}/{res[0].info.run_id}'")
        return res


    def _import_run(self, dst_exp_name, input_dir, dst_notebook_dir):
        exp_id = mlflow_utils.set_experiment(self.mlflow_client, self.dbx_client, dst_exp_name)
        _logger.info(f"Importing run into experiment OUR CUSTOM LOGS RUNNING")
        exp = self.mlflow_client.get_experiment(exp_id)
        src_run_path = os.path.join(input_dir,"run.json")
        src_run_dct = io_utils.read_file_mlflow(src_run_path)
        fs = _filesystem.DatabricksFileSystem()
        run = self.mlflow_client.create_run(exp.experiment_id)
        run_id = run.info.run_id
        try:
            _logger.info("Importing run data")
            run_data_importer.import_run_data(
                self.mlflow_client,
                src_run_dct,
                run_id, 
                self.import_source_tags, 
                src_run_dct["info"]["user_id"],
                self.use_src_user_id, 
                self.in_databricks
            )
            path = os.path.join(input_dir, "artifacts")
            if find_artifacts(run_id, "", "MLmodel"):
                with fs.move_artifacts(path) as path_name:
                    _logger.info("Importing run artifacts")
                    _logger.info(mk_local_path(path))
                    _logger.info(mk_local_path(path_name))
                    _logger.info(f"is model_fix: {self.mlmodel_fix}")
                    if self.mlmodel_fix:
                        _logger.info("Fixing MLmodel")
                        self._update_mlmodel_run_id(run_id)
                    if os.path.exists(mk_local_path(path_name)):
                        _logger.info("Logging artifacts")
                        self.mlflow_client.log_artifacts(run_id, mk_local_path(path_name))
            else:
                _logger.info("No MLmodel found. Skip logging artifacts.")
            _logger.info("Setting run status to FINISHED")
            self.mlflow_client.set_terminated(run_id, RunStatus.to_string(RunStatus.FINISHED))
            run = self.mlflow_client.get_run(run_id)
            if src_run_dct["info"]["lifecycle_stage"] == LifecycleStage.DELETED:
                self.mlflow_client.delete_run(run.info.run_id)
                run = self.mlflow_client.get_run(run.info.run_id)
        except EmptyExperimentPathException as e:
            self.mlflow_client.set_terminated(run_id, RunStatus.to_string(RunStatus.FAILED))
        except Exception as e:
            self.mlflow_client.set_terminated(run_id, RunStatus.to_string(RunStatus.FAILED))
            import traceback
            traceback.print_exc()
            raise MlflowExportImportException(e, f"Importing run {run_id} of experiment '{exp.name}' failed")
        if utils.importing_into_databricks() and dst_notebook_dir:
            ndir = os.path.join(dst_notebook_dir, run_id) if self.dst_notebook_dir_add_run_id else dst_notebook_dir
            self._upload_databricks_notebook(input_dir, src_run_dct, ndir)

        return (run, src_run_dct["tags"].get(MLFLOW_PARENT_RUN_ID, None))


    def _update_mlmodel_run_id(self, run_id):
        """ 
        Workaround to fix the run_id in the destination MLmodel file since there is no method to get all model artifacts of a run.

        Since an MLflow run does not keeps track of its models, there is no method to retrieve the artifact path to all its models.
        This workaround recursively searches the run's root artifact directory for all MLmodel files, and assumes their directory
        represents a path to the model.
        """

        mlmodel_paths = find_artifacts(run_id, "", "MLmodel")
        _logger.info(f"FINDING ARTIFACTS {mlmodel_paths} for {run_id}")
        for mlmodel_path in mlmodel_paths:
            _logger.info("---------------------------------------")
            _logger.info("UPDATING ML MODEL AND LOGGING ARTIFACT")
            _logger.info("{mlmodel_path}")
            _logger.info("---------------------------------------")
            model_path = mlmodel_path.replace("/MLmodel","")
            previous_tracking_uri = mlflow.get_tracking_uri()
            mlflow.set_tracking_uri(self.mlflow_client._tracking_client.tracking_uri)
            local_path = mlflow.artifacts.download_artifacts(
                run_id = run_id,
                artifact_path = mlmodel_path)
            mlflow.set_tracking_uri(previous_tracking_uri)
            _logger.info("---------------------------------------")
            _logger.info(f"{local_path} -- local path")
            _logger.info("---------------------------------------")
            mlmodel = io_utils.read_file(local_path, "yaml")
            mlmodel["run_id"] = run_id
            mlmodel["artifact_path"] = f"artifacts/{mlmodel['artifact_path']}"
            with tempfile.TemporaryDirectory() as dir:
                output_path = os.path.join(dir, "MLmodel")
                io_utils.write_file(output_path, mlmodel, "yaml")
                if model_path == "MLmodel":
                    model_path = ""
                self.mlflow_client.log_artifact(run_id, output_path, model_path)


    def _upload_databricks_notebook(self, input_dir, src_run_dct, dst_notebook_dir):
        run_id = src_run_dct["info"]["run_id"]
        tag_key = "mlflow.databricks.notebookPath"
        src_notebook_path = src_run_dct["tags"].get(tag_key,None)
        if not src_notebook_path:
            _logger.warning(f"No tag '{tag_key}' for run_id '{run_id}'")
            return
        notebook_name = os.path.basename(src_notebook_path)

        format = "source" 
        notebook_path = os.path.join(input_dir,"artifacts","notebooks",f"{notebook_name}.{format}")
        if not os.path.exists(notebook_path): 
            _logger.warning(f"Source '{notebook_path}' does not exist for run_id '{run_id}'")
            return

        with open(notebook_path, "r", encoding="utf-8") as f:
            content = f.read()
        dst_notebook_path = os.path.join(dst_notebook_dir,notebook_name)
        content = base64.b64encode(content.encode()).decode("utf-8")
        data = {
            "path": dst_notebook_path,
            "language": "PYTHON",
            "format": format,
            "overwrite": True,
            "content": content
            }
        mlflow_utils.create_workspace_dir(self.dbx_client, dst_notebook_dir)
        try:
            _logger.info(f"Importing notebook '{dst_notebook_path}' for run {run_id}")
            self.dbx_client._post("workspace/import", data)
        except MlflowExportImportException as e:
            _logger.warning(f"Cannot save notebook '{dst_notebook_path}'. {e}")


@click.command()
@opt_input_dir
@opt_import_source_tags
@opt_experiment_name
@opt_use_src_user_id
@opt_dst_notebook_dir
@click.option("--mlmodel-fix",
    help="Add correct run ID in destination MLmodel artifact. Can be expensive for deeply nested artifacts.", 
    type=bool, 
    default=True, 
    show_default=True
)
@click.option("--dst-notebook-dir-add-run-id",
    help="Add the run ID to the destination notebook workspace directory.",
    type=str,
    required=False,
    show_default=True
)

def main(input_dir, 
        experiment_name, 
        import_source_tags,
        mlmodel_fix, 
        use_src_user_id,
        dst_notebook_dir, 
        dst_notebook_dir_add_run_id):
    _logger.info("Options:")
    for k,v in locals().items():
        _logger.info(f"  {k}: {v}")
    import_run(
        experiment_name = experiment_name,
        input_dir = input_dir,
        dst_notebook_dir = dst_notebook_dir,
        import_source_tags = import_source_tags,
        mlmodel_fix = mlmodel_fix,
        use_src_user_id = use_src_user_id,
        dst_notebook_dir_add_run_id = dst_notebook_dir_add_run_id
    )


if __name__ == "__main__":
    main()
