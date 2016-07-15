#! /usr/bin python3
# -*- coding:utf-8 -*-
import sys

class MHSystemExcept(Exception):
    """MH system exception class"""
    def __init__(self,error):
        self.error = error

    def __unicode__(self):
        return "An unknown error occured: \"{}\". ".format(self.error)
    if sys.version_info > (3, 0):
        def __str__(self):
            return self.__unicode__()

    else:
        def __str__(self):
            return self.__unicode__().encode('utf-8')


class OracleError(MHSystemExcept):
    def __init__(self,ORA_error):
        self.ora = ORA_error

    def __unicode__(self):
        return u"Oracle 数据库产生了一个错误: \"{0}\"".format(self.ora)


class SqliteError(MHSystemExcept):
    def __init__(self,sle_error):
        self.sle = sle_error

    def __unicode__(self):
        return u"Sqlite 数据库产生了一个错误: \"{}\"".format(self.sle)