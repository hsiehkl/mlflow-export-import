"""
Filesystem utilities - local or Databricks
"""

import os
import shutil
import contextlib


def mk_dbfs_path(path):
    return path.replace("/dbfs","dbfs:")


def mk_local_path(path):
    return path.replace("dbfs:","/dbfs")


class EmptyExperimentPathException(Exception):
    pass

class DatabricksFileSystem():
    def __init__(self):
        import IPython
        self.dbutils = IPython.get_ipython().user_ns["dbutils"]

    def ls(self, path):
        return self.dbutils.fs.ls(mk_dbfs_path(path))

    def cp(self, src, dst, recursive=False):
        self.dbutils.fs.cp(mk_dbfs_path(src), mk_dbfs_path(dst), recursive)

    def mv(self, src, dst, recursive=False):
        self.dbutils.fs.mv(mk_dbfs_path(src), mk_dbfs_path(dst), recursive)

    def rm(self, path, recurse=False):
        self.dbutils.fs.rm(mk_dbfs_path(path), recurse)

    def mkdirs(self, path):
        self.dbutils.fs.mkdirs(mk_dbfs_path(path))

    @contextlib.contextmanager
    def move_artifacts(self, path):
        if path.startswith("s3:/"):
            tmp_dst_path = path.replace("s3://", "/dbfs/temp/")
            current_src_path_contents = self.dbutils.fs.ls(mk_dbfs_path(path.replace("/artifacts", "")))
            if len(current_src_path_contents) <=1:
                raise EmptyExperimentPathException
            self.dbutils.fs.cp(mk_dbfs_path(path), mk_dbfs_path(tmp_dst_path), True)
            yield tmp_dst_path
            self.dbutils.fs.rm(mk_dbfs_path(tmp_dst_path), True)
        else:
            yield mk_local_path(path)

    def read(self, path):
        if path.startswith("s3:/"):
            tmp_dst_path = path.replace("s3://", "/dbfs/temp/")
            self.dbutils.fs.cp(mk_dbfs_path(path), mk_dbfs_path(tmp_dst_path), False)
            with open(mk_local_path(tmp_dst_path), "r", encoding="utf-8") as f:
                response = f.read()
            self.dbutils.fs.rm(mk_dbfs_path(tmp_dst_path), False)
        else:
            with open(mk_local_path(path), "r", encoding="utf-8") as f:
                response = f.read()
        return response

    def write(self, path, content):
        self.dbutils.fs.put(mk_dbfs_path(path), content, True)

    def exists(self, path):
        return len(self.ls(path)) > 0
            

class LocalFileSystem():
    def __init__(self):
        pass

    def cp(self, src, dst, recurse=False):
        shutil.copytree(mk_local_path(src), mk_local_path(dst))

    def mv(self, src, dst, recurse=False):
        shutil.move(mk_local_path(src), mk_local_path(dst))

    def rm(self, path, recurse=False):
        shutil.rmtree(mk_local_path(path))

    def mkdirs(self, path):
        os.makedirs(mk_local_path(path),exist_ok=True)

    def read(self, path):
        with open(mk_local_path(path), "r", encoding="utf-8") as f:
            return f.read()

    def write(self, path, content):
        with open(mk_local_path(path), "w", encoding="utf-8") as f:
            f.write(content)

    def exists(self, path):
        return os.path.exists(path)

def get_filesystem(dir):
    """ Return the filesystem object matching the directory path. """
    return DatabricksFileSystem() if (dir.startswith("dbfs:") or dir.startswith("s3:")) else LocalFileSystem()
