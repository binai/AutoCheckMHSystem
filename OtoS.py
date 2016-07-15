#! /usr/bin python3
# -*- coding:utf-8 -*-

from GetOracleData import GetOracle
from SqliteData import UseSqlite
from MHlog import MHLOG


def GetOracleToSqlite(log=None):
    o=GetOracle(log=log)
    s=UseSqlite(log=log)
    for codename in s.Select('codelist','fullname'):
        for fullname, time, value in o.GetData("select t.full_point_id, t.update_time, t.current_value \
                from tb_pub_point_value t \
                where t.full_point_id like '%{codename}'".format(codename=codename[0])):
            value = '{0:.3f}'.format(value)
            date = time.strftime('%c')
            a, style, site, name = o.SiteCode(fullname)
            y, m, d, W, j, h = o.Date_to_Num(time)
            [(vali_min,vali_max)]=s.SelectAll('codelist','min, max',"fullname='{}'".format(a))
            # print(vali_min,vali_max)
            if value > str(vali_max):
                vali = 2
            elif value < str(vali_min):
                vali = 1
            else:
                vali = 0
            s.Insert('mh_temp',(fullname+'_'+j+h, date, value, style, site, name, y, m, d, W, h, vali))
    else:
        o.Close()
        s.Commit()
        s.Close()


if __name__ == "__main__":
    mhlog = MHLOG('mh')
    GetOracleToSqlite(log=mhlog)
