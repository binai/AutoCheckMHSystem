#! /usr/bin python3
# -*- coding:utf-8 -*-

from configparser import ConfigParser,NoSectionError
import os
from io import StringIO

class Configini():
    def __init__(self,filesetini):
        self.inipath=filesetini
        if os.path.isfile(self.inipath):
            self.config = ConfigParser()
            with open(self.inipath, 'r') as f:
                dat = StringIO(f.read())
                dat.seek(0)
                self.config.read_file(dat)

    def Get(self,section,options):
        if section in self.config.sections():
            if type(options) == str:
                try:
                    return self.config.get(section, options)
                except NoSectionError:
                    return None
            else:
                res = []
                for option in options:
                    res.append(self.Get(section,option))
                else:
                    return res
        else:
            if type(options) == str:
                return None
            else:
                return [None]*len(options)