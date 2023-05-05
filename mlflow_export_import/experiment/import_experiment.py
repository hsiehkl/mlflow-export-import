""" 
Exports an experiment to a directory.
"""

import os
import click
import mlflow

from mlflow_export_import.common.click_options import (
    opt_experiment_name,
    opt_input_dir,
    opt_import_source_tags,
    opt_use_src_user_id,
    opt_dst_notebook_dir,
    opt_artifact_location
)
from mlflow_export_import.client.http_client import DatabricksHttpClient
from mlflow_export_import.common import utils, mlflow_utils, io_utils
from mlflow_export_import.common.source_tags import (
    set_source_tags_for_field,
    mk_source_tags_mlflow_tag,
    fmt_timestamps
)
from mlflow_export_import.run.import_run import import_run

_logger = utils.getLogger(__name__)


def import_experiment(
        experiment_name,
        input_dir,
        import_source_tags = False,
        use_src_user_id = False,
        dst_notebook_dir = None,
        mlflow_client = None,
        artifact_location=None
    ):
    """
    :param: experiment_name: Destination experiment name.
    :param: input_dir: Source experiment directory.
    :param import_source_tags: Import source information for MLFlow objects and create tags in destination object.
    :param use_src_user_id: Set the destination user ID to the source user ID.
                            Source user ID is ignored when importing into
    :param mlflow_client: MLflow client.
    :return: A map of source run IDs and destination run.info.
    """
    importer = ExperimentImporter(
        import_source_tags = import_source_tags,
        use_src_user_id = use_src_user_id,
        mlflow_client = mlflow_client
    )
    return importer.import_experiment(
        experiment_name = experiment_name,
        input_dir = input_dir,
        dst_notebook_dir = dst_notebook_dir,
        artifact_location=artifact_location
    )


class ExperimentImporter():

    def __init__(self,
            import_source_tags = False,
            use_src_user_id = False,
            mlflow_client = None,
        ):
        """
        :param mlflow_client: MLflow client.
        :param import_source_tags: Import source information for MLFlow objects and create tags in destination object.
        :param use_src_user_id: Set the destination user ID to the source user ID.
                                Source user ID is ignored when importing into
        """
        self.mlflow_client = mlflow_client or mlflow.client.MlflowClient()
        self.dbx_client = DatabricksHttpClient()
        self.import_source_tags = import_source_tags
        self.use_src_user_id = use_src_user_id


    def import_experiment(self,
            experiment_name,
            input_dir,
            dst_notebook_dir = None,
            artifact_location=None
        ):
        """
        :param: experiment_name: Destination experiment name.
        :param: input_dir: Source experiment directory.
        :return: A map of source run IDs and destination run.info.
        """

        path = io_utils.mk_manifest_json_path(input_dir, "experiment.json")
        exp_dct = io_utils.read_file(path)
        info = io_utils.get_info(exp_dct)
        exp_dct = io_utils.get_mlflow(exp_dct)

        tags = exp_dct["experiment"]["tags"] 
        if self.import_source_tags:
            source_tags = mk_source_tags_mlflow_tag(tags)
            tags = { **tags, **source_tags }
            exp = exp_dct["experiment"]
            set_source_tags_for_field(exp, tags)
            fmt_timestamps("creation_time", exp, tags)
            fmt_timestamps("last_update_time", exp, tags)
        mlflow_utils.set_experiment(self.mlflow_client, self.dbx_client, experiment_name, tags, artifact_location)

        run_ids = exp_dct["runs"]
        run_ids.reverse()
        failed_run_ids = info["failed_runs"]

        _logger.info(f"Importing {len(run_ids)} runs into experiment '{experiment_name}' from '{input_dir}'")
        run_ids_map = {}
        run_info_map = {}
        for src_run_id in run_ids:
            dst_run, src_parent_run_id = import_run(
                mlflow_client = self.mlflow_client,
                experiment_name = experiment_name,
                input_dir = os.path.join(input_dir, src_run_id),
                dst_notebook_dir = dst_notebook_dir,
                import_source_tags = self.import_source_tags,
                use_src_user_id = self.use_src_user_id
            )
            dst_run_id = dst_run.info.run_id
            run_ids_map[src_run_id] = { "dst_run_id": dst_run_id, "src_parent_run_id": src_parent_run_id }
            run_info_map[src_run_id] = dst_run.info
        _logger.info(f"Imported {len(run_ids)} runs into experiment '{experiment_name}' from '{input_dir}'")
        if len(failed_run_ids) > 0:
            _logger.warning(f"{len(failed_run_ids)} failed runs were not imported - see '{path}'")
        utils.nested_tags(self.mlflow_client, run_ids_map)

        return run_info_map


@click.command()
@opt_experiment_name
@opt_input_dir
@opt_import_source_tags
@opt_use_src_user_id
@opt_dst_notebook_dir
@opt_artifact_location

def main(input_dir, experiment_name, import_source_tags, use_src_user_id, dst_notebook_dir, artifact_location):
    _logger.info("Options:")
    for k,v in locals().items():
        _logger.info(f"  {k}: {v}")
    import_experiment(
        experiment_name = experiment_name,
        input_dir = input_dir,
        import_source_tags = import_source_tags,
        use_src_user_id = use_src_user_id,
        dst_notebook_dir = dst_notebook_dir,
        artifact_location=artifact_location
    )


if __name__ == "__main__":
    main()
