#!/usr/bin/env python3
#! _*_ encoding:utf-8 _*_
__author__ = 'Binai'


import cx_Oracle
import time
import datetime
from operator import itemgetter


# 从Oracle数据库中取出电厂编号，电厂名
# 根据电厂编号查找出机组信息
# 分别查找D、M点是否存在对应点
# tb_pub_module_factory/TB_PUB_FACTORY
# v_pub_set
# tb_pub_index tb_pub_point
#



class Oracle_Connect(object):
    def __init__(self,usname,passwd=None,tnsname=None,systemtag=None,log=None):
        self.systemtag=systemtag
        self.log=log
        if passwd:
            self.conn=cx_Oracle.connect(usname,passwd,tnsname)
        else:
            self.conn=cx_Oracle.connect(usname)
        if self.log:
            self.log.info('Oracle初始化连接成功')


    def GetData(self,sql):
        #查询Oracle并返回数据
        cu=self.conn.cursor()
        cu_cache=cu.execute(sql)
        data=cu_cache.fetchall()
        cu_cache.close()
        return data

    def AutoRevise(self,factorys,revises,points):
        sql_m="""SELECT T3.INDEX_CODE,T3.INDEX_NAME FROM TB_PUB_INDEX T3 WHERE T3.FACTORY_CODE='{}' AND T3.SET_CODE={
        } """#M
        sql_d="""SELECT T4.POINT_ID,T4.POINT_NAME FROM TB_PUB_POINT T4 WHERE T4.FACTORY_CODE='{}' AND T4.SET_CODE={
        }"""#D
        pass

    def GetRunState(self,dccode=None,setcode=None):
        sql0="""SELECT T.TABLE_NAME FROM USER_TABLES T WHERE T.TABLE_NAME='TB_PUB_SET_RUN_INFO' """
        sql1='''SELECT DISTINCT T.FACTORY_CODE,T.SET_CODE,RUN_STATE FROM TB_PUB_SET_RUN_INFO T
        WHERE T.IS_ACTIVE='1' AND T.END_TIME > SYSDATE-30
        ORDER BY FACTORY_CODE,SET_CODE ASC '''
        sql2='''SELECT RUN_STATE FROM TB_PUB_SET_RUN_INFO T
        WHERE T.IS_ACTIVE='1' AND T.FACTORY_CODE='{}' AND T.SET_CODE='{}'
        ORDER BY FACTORY_CODE,SET_CODE ASC '''
        sql3='''SELECT T.FACTORY_CODE,T.SET_CODE,RUN_STATE FROM TB_PUB_SET_RUN_INFO T
        WHERE T.IS_ACTIVE='1' AND T.FACTORY_CODE='{}' AND T.END_TIME > SYSDATE-30
        ORDER BY FACTORY_CODE,SET_CODE ASC '''
        #if self.systemtag=='rdlc':
        #    rdlc_dat=Oracle_Connect('ECD','ECD','ECD')
        #    dat=rdlc_dat.GetRunState()
        #    return dat
        if dccode and setcode:
            if self.GetData(sql0):
                dat=self.GetData(sql2.format(dccode,setcode))
                if dat:
                    return dat[0]
                else:
                    return
            else:
                return
        elif dccode:
            if self.GetData(sql0):
                dat=self.GetData(sql3.format(dccode))
                if dat:
                    return {a+'_'+b:c for a,b,c in dat}
                else:
                    return
            else:
                return
        else:
            if self.GetData(sql0):
                dat=self.GetData(sql1)
                if dat:
                    return {a+'_'+b:c for a,b,c in dat}
                else:
                    return
            else:
                return


    def ReviseDict(self,revise=None,allpoint=None,DorM=None,setcode=None,point=None,dccode=None):
        #revise={'D':{'glzgswd':{'glzgswd1':['XT','HP']}},'M':{'glzgswd':{'glzgswd1':['XT','HP']}}
        #allpoint={'fh':'负荷','fdjyggl':'发电机有功功率'}
        if (revise and type(revise) is dict) and (allpoint and type(allpoint) is dict):
            #特殊匹配,在校正中特别标识出来的电厂
            pointname='{}{}_{}'.format(DorM,setcode,point)
            if pointname in allpoint:
                return point
            if dccode:
                if DorM in revise:
                    dat1=revise[DorM]
                    if dat1 and type(dat1) is dict and point in dat1:
                        dat2=[k for k,v in dat1[point].items() if dccode in v]
                        if dat2:
                            return dat2[0]
            #普通匹配
            if DorM in revise and type(revise[DorM]) is dict and point in revise[DorM]:
                for key in revise[DorM][point].keys():
                    pointname='{}{}_{}'.format(DorM,setcode,key)
                    if pointname in allpoint:
                        return key
            return None
        else:
            return None

    # @profile
    def PointAll(self,points=None,revise=None,sqlquery=None):
        """
        提交查询点如 {'D':['glzgswd','fdjyggl','zzgsll'],'M':['FH','zzgswd','e1']}
        提交修正点如 {'D':{'glzgswd':{'glzgswd1':['XT','HP']}},'M':{'glzgswd':{'glzgswd1':['XT','HP']}}
        返回全厂列表['汕头电厂',11,'ST_M3_gpyl','#3机高压缸排汽压力',['ST','M','3','gpyl']]
        """
        #查询是否存在表 TB_PUB_MODULE_FACTORY
        sql0="""SELECT T.TABLE_NAME FROM USER_TABLES T WHERE T.TABLE_NAME='TB_PUB_MODULE_FACTORY' """
        sql1="""SELECT T.TABLE_NAME FROM USER_TABLES T WHERE T.TABLE_NAME='TB_PUB_FACTORY' """
        #sql0="""SELECT T.VIEW_NAME FROM USER_VIEWS T WHERE T.VIEW_NAME='V_PUB_SET'"""
        #查询电厂简称(电厂CODE,电厂名称,电厂标识)
        sql2="""SELECT T1.FACTORY_CODE,T1.FACTORY_NAME,T1.FACTORY_TAG FROM TB_PUB_MODULE_FACTORY T1 """
        sql3="""SELECT T1.FACTORY_CODE,T1.FACTORY_NAME,T1.FACTORY_TAG FROM TB_PUB_FACTORY T1 """
        #sql1="""SELECT DISTINCT T1.FACTORY_CODE,T1.FACTORY_NAME,T1.FACTORY_TAG FROM V_PUB_SET T1"""
        #查询电厂机组信息(1,2)
        sql4="""SELECT T2.SET_CODE FROM V_PUB_SET T2 WHERE T2.SET_CODE!=0 AND T2.FACTORY_CODE='{}' """
        #查询M,D点指标(指标点,指标名称)
        sql5="""SELECT T3.INDEX_CODE,T3.INDEX_NAME FROM TB_PUB_INDEX T3 WHERE T3.FACTORY_CODE='{}' AND T3.SET_CODE={} """#M
        sql6="""SELECT T4.POINT_ID,T4.POINT_NAME FROM TB_PUB_POINT T4 WHERE T4.FACTORY_CODE='{}' AND T4.SET_CODE={}"""#D
        if sqlquery is not None:
            dc_factorys=self.GetData(sqlquery)
        else:
            if self.GetData(sql0):
                dc_factorys=self.GetData(sql2)
            else:
                if self.GetData(sql1):
                    dc_factorys=self.GetData(sql3)
                else:
                    return []

        dc_infos=[]
        runstate_dict=self.GetRunState()
        # print(runstate_dict)
        if dc_factorys:
            for dc in dc_factorys:
                #电厂简称(电厂CODE,电厂名称,电厂标识)
                dc_setcodes=self.GetData(sql4.format(dc[0]))
                for dc_setcode in dc_setcodes:
                    dc_setcode=dc_setcode[0]
                    #开机状态检查
                    if runstate_dict:
                        dc_set_code='{}_{}'.format(dc[0],dc_setcode)
                        # print(dc_set_code)
                        if dc_set_code in runstate_dict:
                            if runstate_dict[dc_set_code]==1:
                                runstat='已停机'
                            elif runstate_dict[dc_set_code]==0:
                                runstat='运行中'
                            else:
                                runstat='状态未知'
                        else:
                            runstat='状态未知'
                    else:
                        runstat='状态未知'
                    #电厂机组信息SET_CODE(1,2)
                    if points and type(points) is dict:
                        #如些格式的点表{'D':['glzgswd','fdjyggl','zzgsll'],'M':['FH','zzgswd','e1']}
                        for item in points:
                            if type(points[item]) is list:
                                #各电厂机组下的点表及点表描述
                                if item=='M':
                                    dc_all_points=self.GetData(sql5.format(dc[0],dc_setcode))
                                elif item=='D':
                                    dc_all_points=self.GetData(sql6.format(dc[0],dc_setcode))
                                else:
                                    dc_all_points=[]
                                if dc_all_points:
                                    dc_all_points={k[0]:k[1] for k in dc_all_points}
                                #各个点匹配
                                for point in points[item]:
                                    #返回值格式['电厂名称','电厂序号','全点名','点描述','运行状态',['电厂缩写','点类型M/D','机组号','点名']]
                                    if self.ReviseDict(revise,dc_all_points,item,dc_setcode,point,dc[0]):
                                        point=self.ReviseDict(revise,dc_all_points,item,dc_setcode,point,dc[0])
                                    dcsetpoint='{}{}_{}'.format(item,dc_setcode,point)
                                    if dcsetpoint in dc_all_points:
                                        pointname=dc_all_points[dcsetpoint]
                                    else:
                                        pointname=''
                                    dcfullpoint='{}_{}{}_{}'.format(dc[0],item,dc_setcode,point)

                                    dc_infos.append([dc[1],dc[2],dcfullpoint,pointname,runstat,[dc[0],item,int(dc_setcode),
                                                                                           point]])
            else:
                try:
                    dc_infos=sorted(dc_infos,key=itemgetter(1,5))
                except TypeError:
                    dc_infos=sorted(dc_infos)
                # print(dc_infos)
                return dc_infos


if __name__=="__main__":
    testpoints={'D':['glzgswd','fdjyggl','zzgsll'],'M':['FH','zzgswd','e1']}
    # con=Oracle_Connect('ECD/ECD@10.150.51.174:1521/orcl')
    con=Oracle_Connect('PUB','PUB','RDLC',systemtag='rdlc')
    a=con.GetRunState()
    print(a)
    #con.PointAll(testpoints)

