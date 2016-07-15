#! /usr/bin python3
# -*- coding:utf-8 -*-

import sqlite3
import os
import re
from MHExcept import *
from Readini import *



class UseSqlite():
    def __init__(self, sqlitefile=None, mh=True, tl=False, tx=False, log=None):
        self.log = log
        self.sqlitefile = sqlitefile
        if not self.sqlitefile:
            self.sqlitefile = 'mh.sqlite'
        if os.path.isfile(self.sqlitefile):
            self.conn = sqlite3.connect(self.sqlitefile)
            self.cur = self.conn.cursor()
        else:
            self.conn = sqlite3.connect(self.sqlitefile)
            self.cur = self.conn.cursor()
        if mh or tl or tx:
            table_list = []
            table_list_temp = [x for (x,) in self.cur.execute('select tbl_name from sqlite_master')]
            self.table_list_temp =table_list_temp
            if mh:
                #
                codelist = '''CREATE TABLE "codelist" ("fullname" TEXT PRIMARY KEY, "min" INTEGER NOT NULL, "max" INTEGER NOT NULL, "style" VARCHAR NOT NULL, "site" INTEGER NOT NULL)'''
                # 临时表
                # 每小时取一次数据，每天取24次数据。并于最后一次汇总至日表，并清除本表。
                mh_temp = '''CREATE TABLE "mh_temp" ("fullname" VARCHAR PRIMARY KEY, "time" time, "value" REAL,"style" VARCHAR, "site" INTEGER NOT NULL, "name" VARCHAR,  "year" INTEGER NOT NULL, "month" INTEGER NOT NULL, "day" INTEGER NOT NULL, "WeekNumber" INTEGER NOT NULL, "hour" INTEGER NOT NULL,"vali" INTEGER NOT NULL DEFAULT 0)'''
                # 日表
                # 每月最后一天汇总当月数据至月表，记录数据至月表数据表中，并清本表。
                mh_day = '''CREATE TABLE "mh_day" ("fullname" VARCHAR PRIMARY KEY, "date" DATE, "value" REAL NOT NULL,"style" VARCHAR, "site" INTEGER NOT NULL,"name" VARCHAR, "year" INTEGER NOT NULL, "month" INTEGER NOT NULL, "day" INTEGER NOT NULL, "WeekNumber" INTEGER NOT NULL,"vali" INTEGER NOT NULL DEFAULT 0)'''
                # 月表
                # 每年最后一天汇总当年数据至年表，
                mh_month = '''CREATE TABLE "mh_month" ("fullname" VARCHAR PRIMARY KEY, "date" DATE, "value" REAL NOT NULL,"style" VARCHAR, "site" INTEGER NOT NULL,"name" VARCHAR, "year" INTEGER NOT NULL, "month" INTEGER NOT NULL,"vali" INTEGER NOT NULL DEFAULT 0)'''
                # 月表数据
                mh_month_data = '''CREATE TABLE "mh_month_data" ("fullname" VARCHAR PRIMARY KEY, "value" TEXT NOT NULL,"style" VARCHAR, "site" INTEGER NOT NULL,"name" VARCHAR, "year" INTEGER NOT NULL, "month" INTEGER NOT NULL,"vali" INTEGER NOT NULL DEFAULT 0)'''
                # 年表
                mh_year = '''CREATE TABLE "mh_year" ("fullname" VARCHAR PRIMARY KEY, "date" DATE, "value" REAL NOT NULL,"style" VARCHAR, "site" INTEGER NOT NULL,"name" VARCHAR, "year" INTEGER NOT NULL,"vali" INTEGER NOT NULL DEFAULT 0)'''
                # 90%负荷数据
                # 每天从web中取一次数据，得到负荷数据
                mh_fh_temp = '''CREATE TABLE "mh_fh_temp" ("datetime" DATE, "value" REAL NOT NULL, "site" INTEGER NOT NULL,"perce" INTEGER NOT NULL, "year" INTEGER NOT NULL, "month" INTEGER NOT NULL, "day" INTEGER NOT NULL)'''
                # 汇总每月90%负荷数据
                mh_fh_month = ''''''
                table_list += ['codelist','mh_temp','mh_day','mh_month','mh_month_data']
            if tl:
                table_list += []
            if tx:
                table_list += []
            for t in set(table_list) - set(table_list_temp):
                # print(t)
                # print(eval(t))
                self.cur.execute(eval(t))
            else:
                self.conn.commit()
        if os.path.isfile('setini.ini'):
            self.config = Configini('setini.ini')
            codeset, codestyle=self.config.Get('mh',['set', 'style'])
            self.set = codeset.replace(' ','').split(',')
            self.style = codestyle.replace(' ','').split(',')
            if self.set:
                for site in self.set:
                        if "view_D{site}".format(site=site) not in self.table_list_temp:
                            self.cur.execute('CREATE VIEW "view_D{site}" AS select distinct(name) from mh_temp t where t.site = {site} and t.style = "D"'.format(site=site))
                        if "view_M{site}".format(site=site) not in self.table_list_temp:
                            self.cur.execute('CREATE VIEW "view_M{site}" AS select distinct(name) from mh_temp t where t.site = {site} and t.style = "M"'.format(site=site))
                self.SetView()


    def SetView(self):
        if self.set:
            for style in self.style:
                for site in self.set:
                    exec("self.code{style}{site} = self.config.Get('mh_code','code{style}{site}')".format(style=style,site=site))
                    exec("self.code{style}{site}_Min = self.config.Get('mh_code','code{style}{site}_Min').replace(' ','').split(',')".format(style=style,site=site))
                    exec("self.code{style}{site}_Max = self.config.Get('mh_code','code{style}{site}_Max').replace(' ','').split(',')".format(style=style,site=site))
            else:
                res = {k:v for k,v in self.__dict__.items() if 'code' in k}
                # print(self.__dict__)
                # print(res)
                # resD = sorted([k for k in res.keys() if 'codeD' in k])
                # resD_list = [(resD[x*3], resD[x*3+1], resD[x*3+2]) for x in range(int(len(resD)/3))]
                # resM = sorted([k for k in res.keys() if 'codeM' in k])
                # resM_list = [(resM[x*3], resM[x*3+1], resM[x*3+2]) for x in range(int(len(resM)/3))]
                # print(resD_list, resM_list)
                resDM = sorted([k for k in res.keys() if 'code' in k])
                # ['codeD1', 'codeD1_Max', 'codeD1_Min', 'codeD2', 'codeD2_Max', 'codeD2_Min', 'codeM1', 'codeM1_Max', 'codeM1_Min', 'codeM2', 'codeM2_Max', 'codeM2_Min']
                # print(resDM)
                resDM_list = [(resDM[x*3], resDM[x*3+1], resDM[x*3+2]) for x in range(int(len(resDM)/3))]
                # [('codeD1', 'codeD1_Max', 'codeD1_Min'), ('codeD2', 'codeD2_Max', 'codeD2_Min'), ('codeM1', 'codeM1_Max', 'codeM1_Min'), ('codeM2', 'codeM2_Max', 'codeM2_Min')]
                # print(resDM_list)
                for x in resDM_list:
                    self.CreatView(x)

    def SiteCode(self,code):
        s=re.search(r'([M|D|H])(\d{1,2})_(\S+)$',code)
        if s:
            #fullname, style, site, name
            return s.group(0), s.group(1), s.group(2), s.group(3)
        else:
            return None, None, None, None

    def CreatView(self,options):
        a, b, c = [x.replace(' ','').split(',') for x in self.config.Get('mh_code', options)]
        # list_a, list_b, list_c = a.replace(' ','').split(','), b.replace(' ','').split(','), c.replace(' ','').split(',')
        ress = list(zip(a,b,c))
        # print(ress)
        fullnamelist = [x for (x,) in self.SelectAll('codelist', 'fullname')]
        for res in ress:
            _, style, site, name=self.SiteCode(res[0])
            if "view_{style}{site}_{name}".format(style=style, site=site, name=name) not in self.table_list_temp:
                self.cur.execute('CREATE VIEW "view_{style}{site}_{name}" AS \
                    select * from mh_temp t \
                    where t.site = {site} and t.style = "{style}" and t.name="{name}" and t.WeekNumber = strftime("%W","now") and t.day = strftime("%d","now")\
                    order by t.fullname asc'.format(style=style, site=site, name=name))
            if "view_{style}{site}_{name}_week".format(style=style, site=site, name=name) not in self.table_list_temp:
                self.cur.execute('CREATE VIEW "view_{style}{site}_{name}_week" AS \
                    select * from mh_temp t \
                    where t.site = {site} and t.style = "{style}" and t.name="{name}" and t.WeekNumber = strftime("%W","now")\
                    order by t.fullname asc'.format(style=style, site=site, name=name))
            if res[0] not in fullnamelist:
                self.Insert('codelist',(res[0], res[2], res[1], style, site))
            else:
                self.Update('codelist',{'min':res[2],'max':res[1]},{'fullname':res[0]})
        else:
            self.Commit()

    def Close(self):
        try:
            self.cur.close()
            self.conn.close()
        except BaseException:
            pass


    def Commit(self):
        self.conn.commit()

    def Insert(self,tablename,values):
        #插入数据 tablename = 表名; values = (1,2,3,4,5,6,7,8,9,10,11)
        try:
            self.cur.execute("insert into {tablename} values {values}".format(tablename=tablename, values=values))
        except sqlite3.OperationalError as e:
            raise SqliteError(e)
        except (sqlite3.IntegrityError) as e:
            # print(SqliteError((e,values)))
            pass

    def Update(self,tablename,values=None,where=None):
        sql = "update {tablename} set {values} where {where}"
        if type(values) == dict:
            values = ','.join(['{k}={v}'.format(k=k, v=v) for k, v in values.items()])
        if type(where) == dict and len(where) == 1:
            where = ''.join(['{k}="{v}"'.format(k=k, v=v) for k, v in where.items()])
        try:
            self.cur.execute(sql.format(tablename=tablename,values=values,where=where))
        except sqlite3.OperationalError as e:
            if self.log:
                self.log.Info(e)
            else:
                raise SqliteError(e)

    def Select(self,tablename, output='*', where=None, order=None):
        sql = "select {output} from {tablename}"
        if where:
            sql += ' where {where}'
            if type(where) == dict and len(where) == 1:
                where = ''.join(['{k}="{v}"'.format(k=k, v=v) for k, v in where.items()])
        if order:
            sql += ' order by {order}'
        try:
            self.cur.execute(sql.format(tablename=tablename,output=output,where=where,order=order))
            for res in self.cur.fetchall():
                yield res
        except sqlite3.OperationalError as e:
            if self.log:
                self.log.Info(e)
            else:
                raise SqliteError(e)

    def SelectAll(self,tablename, output='*', where=None, order=None):
        sql = "select {output} from {tablename}"
        if where:
            sql += ' where {where}'
            if type(where) == dict and len(where) == 1:
                where = ''.join(['{k}="{v}"'.format(k=k, v=v) for k, v in where.items()])
        if order:
            sql += ' order by {order}'
        try:
            # print(sql)
            self.cur.execute(sql.format(tablename=tablename,output=output,where=where,order=order))
            return self.cur.fetchall()
        except sqlite3.OperationalError as e:
            if self.log:
                self.log.Info(e)
            else:
                raise SqliteError(e)

    def Delete(self,tablename, where=None):
        sql = "delete from {tablename}"
        if where:
            sql += ' where {where}'
            if type(where) == dict and len(where) == 1:
                where = ''.join(['{k}="{v}"'.format(k=k, v=v) for k, v in where.items()])
        try:
            self.cur.execute(sql.format(tablename=tablename,where=where))
            self.Commit()
        except sqlite3.OperationalError as e:
            if self.log:
                self.log.Info(e)
            else:
                raise SqliteError(e)


if __name__ == '__main__':
    s = UseSqlite()
    # s.Insert('mh_temp',(0,1,2,3,4,5,6,7,8))
    # s.Commit()
    str = list(s.Select('mh_temp'))
    # print(str)
