import os
import sysconfig
from setuptools import setup, find_packages
from Cython.Build import cythonize

import noidd


PACKAGE="noidd"

def open_local(paths, mode="r", encoding="utf8"):
    path = os.path.join(os.path.abspath(os.path.dirname(__file__)), *paths)

    return codecs.open(path, mode, encoding)


with open_local([PACKAGE, "__init__.py"]) as fp:
    try:
        version = re.findall(
            r"^__version__ = \"([^']+)\"\r?$", fp.read(), re.M
        )[0]
    except IndexError:
        raise RuntimeError("Unable to determine version.")

with open_local("requirements.txt") as fp:
    try:
        install_requires = fp.readlines()
    except IndexError:
        raise RuntimeError("unable to get requirements")

setup(
    name='genrandom',
    version=version,
    package_dir = {'':'noidd'}
    install_requires=install_requires
)

