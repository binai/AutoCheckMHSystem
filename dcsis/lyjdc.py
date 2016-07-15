#!/usr/bin/env python3
#! _*_ encoding:utf-8 _*_
__author__ = 'Binai'
__dcsite__ = '鲤鱼江电厂'


import urllib.request,urllib.parse
import http.cookiejar
import bs4,chardet,os,re,time


class DTXY(object):
    def __init__(self,offset=None,sec=5,log=None):
        #创建self.opener便于后续进行访问操作
        self.sec=sec
        if type(offset) is str:
            self.systemname=offset.lower()
        elif type(offset) is dict:
            self.systemname=offset['systemname'] if 'systemname' in offset else None
        self.log=log

    def CheckWebStatus(self):
        #模拟访问WEB，成功返回True
        if self.systemname==None:
            raise ValueError
        elif self.systemname=='mh':
            #http://10.192.113.42/sis50/core/modules/logon/logon.aspx
            f=self.systemname+'.txt'
            first_url='http://10.192.113.42/sis50/Core/Modules/Logon/logon.aspx?LoginType=1&UserID=hpfdc&Password=888888'
            second_url='http://10.192.113.42/sis50/core/Main/main.aspx'
            first_headers={'Host':"10.192.113.42",
             'User-Agent':"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)",
             'Referer':"http://10.192.113.42/sis50/Core/Modules/Logon/logon.aspx?LoginType=1&UserID=hpfdc&Password=888888",
             'Connection':"keep-alive"}
            second_headers={'Host':"10.192.113.42",
             'User-Agent':"Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)",
             'Referer':"http://10.192.113.42/sis50/Core/Modules/Logon/logon.aspx?LoginType=1&UserID=hpfdc&Password=888888",
             'Connection':"keep-alive"}
            data={'username':'hpfdc',
              'password':'888888',
              'ImageButton_login.x':'36',
              'ImageButton_login.y':'13'}

        if f:
            #cookie文件位置
            if os.path.isdir(os.path.dirname(__file__)):
                f=os.path.join(os.path.dirname(__file__),f)
            else:
                f=os.path.join(os.getcwd(),f)
            # print(f)
            if os.path.isfile(f):
                cookie = http.cookiejar.MozillaCookieJar()
                cookie.load(filename=f,ignore_discard=True,ignore_expires=True)
            else:
                cookie=http.cookiejar.MozillaCookieJar(f)
        else:
            cookie=http.cookiejar.CookieJar()
        handler=urllib.request.HTTPCookieProcessor(cookie)
        self.opener=urllib.request.build_opener(handler)
        #第一次打开网站，获取编码等必要信息
        first_url_headers=urllib.request.Request(first_url,headers=first_headers,method='GET')
        first_res=self.opener.open(first_url_headers,timeout=30)
        if f:cookie.save(filename=f,ignore_discard=True,ignore_expires=True)
        first_html=first_res.read()
        charcode=chardet.detect(first_html)
        if charcode['encoding']:
            self.charcode=charcode['encoding']
        else:
            self.charcode='utf-8'
        soup=bs4.BeautifulSoup(first_html.decode(self.charcode,errors='ignore'),'html.parser')

        for item in soup.findAll(attrs={'type':'hidden'}):
            data[item['name']]=item['value']
        # print(self.data)
        #第二次请求，模拟正常访问提交POST数据
        first_fullurl=urllib.request.Request(first_url,
                                    data=urllib.parse.urlencode(data,encoding=self.charcode).encode(self.charcode),
                                    headers=first_headers,method='POST')
        self.opener.open(first_fullurl,timeout=30).read()
        if f:cookie.save(filename=f,ignore_discard=True,ignore_expires=True)
        second_fullurl=urllib.request.Request(second_url,headers=second_headers)
        #第三次请求，正常打开网站，可获取到数据
        self.second_res=self.opener.open(second_fullurl,timeout=30)
        self.headers=second_headers
        if f:cookie.save(filename=f,ignore_discard=True,ignore_expires=True)
        #是否成功登录，登录成功，打印对应系统名称，首页及其地址
        second_html=self.second_res.read()
        second_soup=bs4.BeautifulSoup(second_html.decode(self.charcode,errors='ignore'),'html.parser')
        # print(second_soup.title.string)
        # print(second_soup.body.form.table)
        # print(second_soup.body['onload'])
        if second_soup.title.string.strip() in ['广东省节能发电调度在线监测系统子站'] \
                and second_soup.body.form.table.find('子站')!=-1:
            print('{}\t{} 登录成功'.format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()),
                                       self.systemname.upper()))

            if self.log:
                self.log.info('{}登录成功'.format(self.systemname))
            #---------------------
            r=['-','/','|','\\']
            n=0
            sec=self.sec
            while True:
                n1=n%4
                n2=int((sec*4-n)/4)
                print('\t{}  {}\tJust a moment, please.'.format(r[n1],n2),end='\r')
                time.sleep(.25)
                n+=1
                if n>(4*sec):break
            #---------------------
            return True
        else:
            print('{}\t{} 登录失败'.format(time.strftime('%Y-%m-%d %H:%M:%S',time.localtime()),
                                       self.systemname.upper()))
            if self.log:
                self.log.info('{}登录失败'.format(self.systemname))
            return False


if __name__=='__main__':
    dt_rout=DTXY('mh')
    dt_rout.CheckWebStatus()
    # dt_data=dt_rout.CheckRealPoint('D','5','fdjyggl')
    # dt_data=dt_rout.CheckRealPoint('TG','D','1','glzgswd')
    #dt_data=dt_rout.CheckRealPoint('PH','M','1','FH')
    # dt_data=dt_rout.CheckRealPoint('HM','D','3','zzgsll')
    # dt_data=dt_rout.CheckRealPoint('CZ','M','4','zzgswd')
    #dt_weed=dt_rout.RealDataWeed(dt_data)
    #print(dt_weed)







