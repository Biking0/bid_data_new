import pandas as pd
from sqlalchemy.engine import create_engine
eg = create_engine('hive://10.1.236.80:10000/default')
pd.read_sql('show tables', eg)