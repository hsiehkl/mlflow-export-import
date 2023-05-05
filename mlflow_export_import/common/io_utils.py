import os
import getpass
import json
import yaml

from mlflow_export_import.common.timestamp_utils import ts_now_seconds, ts_now_fmt_utc
from mlflow_export_import.common import filesystem as _filesystem
from mlflow_export_import.common.source_tags import ExportFields
from mlflow_export_import.common.pkg_version import get_version #


def _mk_system_attr(script):
    """
    Create system JSON stanza containing internal export information.
    """
    import mlflow
    import platform
    dct = {
        "package_version": get_version(),
        "script": os.path.basename(script),
        "export_time": ts_now_seconds,
        "_export_time": ts_now_fmt_utc,
        "mlflow_version": mlflow.__version__,
        "mlflow_tracking_uri": mlflow.get_tracking_uri(),
        "platform": {
            "python_version": platform.python_version(),
            "system": platform.system(),
            "processor": platform.processor()
        },
        "user": getpass.getuser(),
    }
    dbr = os.environ.get("DATABRICKS_RUNTIME_VERSION", None)
    if dbr:
        dct2 = {
            "databricks": {
                 "DATABRICKS_RUNTIME_VERSION": dbr,
            }
        }
        dct = { **dct, **dct2 }
    return { ExportFields.SYSTEM: dct }


def write_export_file(dir, file, script, mlflow_attr, info_attr=None):
    """
    Write standard formatted JSON file.
    """
    dir = _filesystem.mk_local_path(dir)
    fs = _filesystem.get_filesystem(dir)
    path = os.path.join(dir, file)
    info_attr = { ExportFields.INFO: info_attr} if info_attr else {}
    mlflow_attr = { ExportFields.MLFLOW: mlflow_attr}
    mlflow_attr = { **_mk_system_attr(script), **info_attr, **mlflow_attr }
    fs.mkdirs(dir)
    write_file(path, mlflow_attr)


def _is_yaml(path, file_type=None):
    return any(path.endswith(x) for x in [".yaml",".yml"]) or file_type in ["yaml","yml"]


def write_file(path, content, file_type=None):
    """
    Write to JSON, YAML or text file.
    """
    path = _filesystem.mk_local_path(path)
    fs = _filesystem.get_filesystem(path)
    if path.endswith(".json"):
        fs.write(path, json.dumps(content, indent=2)+"\n")
    elif _is_yaml(path, file_type):
        fs.write(path, yaml.dump(content))
    else:
        fs.write(path, content)


def read_file(path, file_type=None):
    """
    Read a JSON, YAML or text file.
    """
    fs = _filesystem.get_filesystem(path)
    contents = fs.read(path)
    if path.endswith(".json"):
        return json.loads(contents)
    elif _is_yaml(path, file_type):
        return yaml.safe_load(contents)
    else:
        return contents


def get_info(export_dct):
    return export_dct[ExportFields.INFO]


def get_mlflow(export_dct):
    return export_dct[ExportFields.MLFLOW]


def read_file_mlflow(path):
    dct = read_file(path)
    return dct[ExportFields.MLFLOW] 


def mk_manifest_json_path(input_dir, filename):
    return os.path.join(input_dir, filename)
