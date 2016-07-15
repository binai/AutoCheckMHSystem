from OracleConn import *
from WebCheck import *
import time


testpoints={'D':['glzgswd','fdjyggl','zzgsll'],'M':['FH','zzgswd','e1']}
con=Oracle_Connect('ECD/ECD@10.150.51.174:1521/orcl')
data=con.PointAll(testpoints)

dt_rout=DTXY('reapuum')
dt_rout.CheckWebStatus()
for v in data:
    dt_data=dt_rout.CheckRealPoint(*v[-1])
    dt_weed=dt_rout.RealDataWeed(dt_data,unvalue=(10,0))
    print(dt_weed)
    time.sleep(.1)
