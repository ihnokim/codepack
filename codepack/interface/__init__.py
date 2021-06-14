import codepack.interface.mongodb
import codepack.interface.mysql
import codepack.interface.mssql
import numpy as np


def isnan(value):
    ret = False
    try:
        ret = np.isnan(value)
    except Exception:
        pass
    finally:
        return ret


MongoDB = mongodb.MongoDB
MySQL = mysql.MySQL
MSSQL = mssql.MSSQL
