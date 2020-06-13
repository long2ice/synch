import re


def version():
    with open("pyproject.toml") as f:
        ret = re.findall('version = "(\d+\.\d+\.\d+)"', f.read())
        return ret[0]


__version__ = version()
