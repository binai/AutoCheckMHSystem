import OracleConn
import WebCheck
import pickle,os
import psutil,platform

if platform.platform().find('Windows-2008')!=-1:
    try:
        from gevent.pool import Pool as ThreadPool
    except ImportError:
        from multiprocessing.dummy import Pool as ThreadPool
else:
    from multiprocessing.dummy import Pool as ThreadPool

SQLPWD={'reapuum':('ECD','ECD','ECD'),
        'rdlc':('PUB','PUB','RDLC'),
        'dnlms':('ECD','ECD','DNLMS_174'),
        'dolms':('ECD','ECD','DOLMS174')}
SQL={'dolms':'SELECT T1.FACTORY_CODE,T1.FACTORY_FLAG,T1.FACTORY_TAG FROM TB_PUB_MODULE_FACTORY T1'}
dcpoints={'reapuum':{'D':['glzgswd','fdjyggl','zzgsll'],'M':['FH','zzgswd','e1']},
          'rdlc':{'D':['fdjyggl'],'M':['FH','zzgswd','e1']},
          'dnlms':{'D':['fdjyggl','zzgsll'],'M':['FH','zzgswd','e1']},
          'dolms':{'D':['fdjyggl','zzgsll'],'M':['FH','zzgswd','e1']}}
#dcpoints={'D':['fdjyggl'],'M':['FH']}
# revisepoints={'D':{'glzgswd':{'glzgswd1':[]}},'M':{'glzgswd':{'glzgswd1':[]}}}
revisepoints={'reapuum':{'D':{},'M':{}},
              'rdlc':{'D':{},'M':{}},
              'dnlms':{'D':{},'M':{}},
              'dolms':{'D':{},'M':{}}}


def PoolGetDat(func):
    def RUN(data):
        value=data
        res_data1=func.CheckRealPoint(*value[-1])
        res_data2=func.RealDataWeed(res_data1,unvalue=(10,0))
        value[-1]=res_data2
        return value
    return RUN


#WEB取数多线程
def PoolOracleCon(SQLPWD,SQL=None,dcpoints=None,revisepoints=None,multi=4,log=None):
    def RUN(systemstyle):
        dcpoints_poc,revisepoints_poc,SQL_poc=None,None,None
        if SQL:
            if systemstyle in  SQL:
                SQL_poc=SQL[systemstyle]
        if dcpoints:
            if systemstyle in dcpoints:
                dcpoints_poc=dcpoints[systemstyle]
        if revisepoints:
            if systemstyle in revisepoints:
                revisepoints_poc=revisepoints[systemstyle]
        con=OracleConn.Oracle_Connect(*SQLPWD[systemstyle],systemtag=systemstyle,log=log)
        data=con.PointAll(dcpoints_poc,revisepoints_poc,SQL_poc)
        dtxy_out=WebCheck.DTXY(systemstyle,log=log)
        dtxy_out.CheckWebStatus()
        pooloracle=ThreadPool(multi)
        poolgetdat=PoolGetDat(dtxy_out)
        oracle_res=pooloracle.map(poolgetdat,data)
        if str(type(pooloracle)).find('multiprocessing.pool.ThreadPool')>-1:
            pooloracle.close()
            pooloracle.join()
        filename='{}.pickle'.format(systemstyle)
        if os.path.isdir(os.path.dirname(__file__)):
            thefilename=os.path.join(os.path.dirname(__file__),filename)
        else:
            thefilename=os.path.join(os.getcwd(),filename)
        with open(thefilename,'wb') as f:
            pickle.dump(oracle_res,f)
        return oracle_res
    return RUN

#各系统循环
def PoolWebCon(SQLPWD,SQL,dcpoints,revisepoints,log=None):
    systemstyles=list(SQLPWD.keys())
    len_multi=len(systemstyles)
    len_submulti=int((psutil.cpu_count()-2)/(len_multi-1))
    if len_submulti<4:len_submulti=4
    poolweb=ThreadPool(len_submulti)
    pooloraclecon=PoolOracleCon(SQLPWD,SQL,dcpoints,revisepoints,len_submulti,log=log)
    web_res=poolweb.map(pooloraclecon,systemstyles)
    if str(type(poolweb)).find('multiprocessing.pool.ThreadPool')>-1:
        poolweb.close()
        poolweb.join()
    return web_res


if __name__=="__main__":
    res=PoolWebCon(SQLPWD,SQL,dcpoints,revisepoints)
    print(res)




