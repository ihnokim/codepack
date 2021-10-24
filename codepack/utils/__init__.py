from codepack.utils.looper import Looper
from configparser import ConfigParser


def get_config(filename, section):
    cp = ConfigParser()
    cp.read(filename)
    items = cp.items(section)
    return {item[0]: item[1] for item in items}
