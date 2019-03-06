from ProPackage.ProMySqlDB import ProMySqlDB
from ProPackage.LangMySQL import *
from ProPackage.ProConfig import *

import pandas as pd
import numpy as np


if __name__ == '__main__':
    mySqlDBLocal = ProMySqlDB(mySqlDBC_EARNINGDB_Name, mySqlDBC_UserLocal,
                              mySqlDBC_Passwd, mySqlDBC_HostLocal, mySqlDBC_Port)