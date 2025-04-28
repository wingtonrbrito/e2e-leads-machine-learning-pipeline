from contextlib import contextmanager
from pathlib import PosixPath
import shutil
import tempfile


@contextmanager
def tempdir(prefix='tmp'):
    """
    tempDir method is duplicated. These files inside /lib must remain separated,
    because they are deployed into cloud composer,
    and will not be able to reference any other files outside the dags folder.
    Stu. M 11/28 - extracted from here:  https://stackoverflow.com/q/10965479/20178
    """
    tmpdir = tempfile.mkdtemp(prefix=prefix)
    try:
        yield tmpdir
    finally:
        shutil.rmtree(tmpdir)


def parse_template(file_path, **kwargs):
    template = PosixPath(file_path).read_text()
    template = template.format(**kwargs)
    return template
