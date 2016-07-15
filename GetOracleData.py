#! /usr/bin python3
# -*- coding:utf-8 -*-

import cx_Oracle
import datetime
import re
from MHExcept import *
from Readini import *

class GetOracle():
    def __init__(self,user=None,pwd=None,tnsname=None,log=None):
        self.user = user
        self.pwd = pwd
        self.tnsname = tnsname
        self.log = log
        if not self.user :
            if os.path.isfile('setini.ini'):
                self.config = Configini('setini.ini')
                self.user,self.pwd,self.tnsname=self.config.Get('oracle',['user', 'pwd', 'tnsname'])
            else:
                self.user = 'SIS'
                self.pwd = 'SIS'
                self.tnsname = 'SIS'
        if (self.user and self.pwd and self.tnsname):
            #print(type(self.user),type(self.tnsname),type(self.tnsname))
            try:
                self.conn = cx_Oracle.connect(self.user, self.pwd, self.tnsname)
            except cx_Oracle.DatabaseError as e:
                if self.log:
                    self.log.Info(e)
                else:
                    raise OracleError(e)
        elif self.user:
            try:
                self.conn = cx_Oracle.connect(self.user)
            except cx_Oracle.DatabaseError as e:
                if self.log:
                    self.log.Info(e)
                else:
                    raise OracleError(e)
        if self.log:
            self.log.Info('Oracle初始化连接成功')

    def GetData(self, sql):
        # 查询 Oracle 并返回数据
        if self.conn:
            self.cur = self.conn.cursor()
            self.cur_cache=self.cur.execute(sql)
            for res in self.cur_cache:
                yield res
            else:
                self.cur_cache.close()
        else:
            if self.log:
                self.log.Info("Oracle未连接!")
            else:
                raise OracleError("Oracle未连接!")

    def Close(self):
        #关闭 Oralce 连接
        try:
            self.cur_cache.close()
            self.cur.close()
            self.conn.close()
        except BaseException:
            pass

    def Date_to_Num(self,strftime):
        #return 年, 月, 日, 第几周, 第几日, 时分
        return (strftime.strftime('%Y'), strftime.strftime('%m'), strftime.strftime('%d'), strftime.strftime('%U'), strftime.strftime('%j'), strftime.strftime('%H%M'))

    def SiteCode(self,code):
        s=re.search(r'([M|D|H])(\d{1,2})_(\S+)$',code)
        if s:
            #fullname, style, site, name
            return s.group(0), s.group(1), s.group(2), s.group(3)
        else:
            return None, None, None, None


if __name__ == '__main__':
    i=GetOracle()
    for x in i.GetData('select t.full_point_id, t.update_time, t.current_value from tb_pub_point_value t'):
        print(x)
    # c=cx_Oracle.connect('ECD','ECD','ECD')
