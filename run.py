#!/usr/bin/env python3
#! _*_ encoding:utf-8 _*_
__author__ = 'Binai'

import PoolRun,os,XlsxCreate
import time,logging
from logging.handlers import RotatingFileHandler
from urllib.error import *

'''
所有字键均为数据库访问所标示出的字键为准，包括产生的cookie
数据库访问  {'reapuum':('ECD','ECD','ECD')}
数据库查询修正 {'reapuum':'sql query'}
提交查询点如 {'D':['glzgswd','fdjyggl','zzgsll'],'M':['FH','zzgswd','e1']}
提交修正点如 {'D':{'glzgswd':{'glzgswd1':['XT','HP']}},'M':{'glzgswd':{'glzgswd1':['XT','HP']}}
'''
oracle_access={'reapuum':('ECD','ECD','ECD'),
        'rdlc':('PUB','PUB','RDLC'),
        'dnlms':('ECD','ECD','DNLMS_174'),
        'dolms':('ECD','ECD','DOLMS174')}

oracle_query={'dolms':'SELECT T1.FACTORY_CODE,T1.FACTORY_FLAG,T1.FACTORY_TAG FROM TB_PUB_MODULE_FACTORY T1'}

all_dc_points={'reapuum':{'D':['glzgswd','fdjyggl','cyqrkll'],'M':['FH','zzgswd','fdbmF']},
          'rdlc':{'D':['fdjyggl'],'M':['FH']},
          'dnlms':{'D':['fdjyggl'],'M':['FH']},
          'dolms':{'D':['fdjyggl'],'M':['FH']}}

all_revise_points={'reapuum':{'D':{'fdjyggl':{'fdjyggl1':[],'fdjyggl2':[],'fdjyggl3':[],'fdjyggl4':[]},
                                   'glzgswd':{'glzgswd1':[],'glzgswd2':[]},
                                   'cyqrkll':{'cyqrkll1':['RZB','HYG','MM','TS'],'cyqrkll3':['HM'],'cyqjkll':['ZHA']}},
                              'M':{}},
              'rdlc':{'D':{'fdjyggl':{'fdjyggl1':[],'fdjyggl2':[],'rd_fdjyggl':['RZB','DS','XY','ZX']}},
                      'M':{}},
              'dnlms':{'D':{'fdjyggl':{'fdjyggl1':[],'fdjyggl2':[],'fdjyggl3':[],'fdjyggl4':[],'tx_fdjyggl':[],
                                       'swyggl':['SJA']}},
                       'M':{}},
              'dolms':{'D':{'fdjyggl':{'fdjyggl1':[],'fdjyggl2':[]}}
                      ,'M':{}}}

pickle_filename=['reapuum.pickle','rdlc.pickle','dnlms.pickle','dolms.pickle']
data_str=time.strftime('%Y\\u5e74%m\\u6708%d\\u65e5',time.localtime()).encode('utf-8').decode('raw-unicode-escape')
xlsxset={'xlsxname':'广东中调电厂数据点巡检报表{}'.format(time.strftime('%Y\\u5e74%m\\u6708%d\\u65e5',
                                                           time.localtime()).encode('utf-8').decode('raw-unicode-escape')),
                  'head':{'reapuum':'广东省节能发电调度煤耗在线监测系统',
                          'rdlc':'广东电网热电联产在线监测系统',
                          'dnlms':'广东电网脱硝在线监测系统',
                          'dolms':'广东电网脱硫在线监测系统'},
                  'subhead':{'reapuum':'煤耗项目巡检   {}'.format(data_str),
                            'rdlc':'热电项目巡检   {}'.format(data_str),
                            'dnlms':'脱硝项目巡检   {}'.format(data_str),
                            'dolms':'脱硫项目巡检   {}'.format(data_str)},
                  'worksheet':{'reapuum':'煤耗巡检','rdlc':'热电巡检','dnlms':'脱硝巡检','dolms':'脱硫巡检'}}




time1=time.time()
try:
    if os.path.isdir(os.path.dirname(__file__)):
        error_log=os.path.join(os.path.dirname(__file__),'error.log')
    else:
        error_log=os.path.join(os.getcwd(),'error.log')
except NameError:
    error_log=os.path.join(os.getcwd(),'error.log')
log=logging.getLogger('DTXY')
log.setLevel(logging.INFO)
rthandler=RotatingFileHandler(error_log,'a',maxBytes=1024*4096,backupCount=3)
formatter=logging.Formatter('%(asctime)s : %(levelname)-8s---- %(message)s')
rthandler.setFormatter(formatter)
log.addHandler(rthandler)

num=0
while True:
    if num>10:break
    try:
        res=PoolRun.PoolWebCon(oracle_access,oracle_query,all_dc_points,all_revise_points,log=log)
        f=XlsxCreate.CreateXLSX(pickle_filename,xlsxset)
        if res:
            thefile=f.createxlsx()
            if thefile:
                log.info('生成{}文件成功'.format(thefile))
                break
    except (ConnectionResetError,HTTPError) as e:
        print(e)
        log.exception(e)
        num+=1
        time.sleep(5*60)
    except Exception as e:
        print(e)
        log.exception(e)
        num+=1
time2=time.time()
log.info('总耗时{}'.format(time2-time1))
