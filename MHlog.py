#! /usr/bin python3
# -*- coding:utf-8 -*-


import logging
from logging.handlers import RotatingFileHandler

class MHLOG():
    def __init__(self,filename):
        if not '.log' in filename:
            filename+='.log'
        self.log=logging.getLogger(filename)
        self.log.setLevel(logging.INFO)
        rthandler=RotatingFileHandler(filename,'a',maxBytes=1024*1024*1,backupCount=3)
        formatter=logging.Formatter('%(asctime)s : %(levelname)-8s---- %(message)s')
        rthandler.setFormatter(formatter)
        self.log.addHandler(rthandler)

    def Info(self,info):
        self.log.info(info)