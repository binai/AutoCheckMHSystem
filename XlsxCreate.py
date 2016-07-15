#!/usr/bin/env python3
#! _*_ encoding:utf-8 _*_
__author__ = 'Binai'

import xlsxwriter
import pickle,re,os,time,shutil
from tempfile import mkstemp

class CreateXLSX(object):
    def __init__(self,picklefiles,workname={}):
        #输入pickle文件名{'reapuum':r'c:/reapuum.pickle','rdlc':r'd:/rdlc.pickle'}
        # worksheetname表名{'xlsxname':'巡检报表',
        # 'head':{'reapuum':'煤耗巡检'},
        # 'subhead':{'reapuum':'煤耗巡检'}
        # 'worksheet':{'reapuum':'广东电网煤耗数据巡检','rdlc':'热电联产'}}
        self.picklefiles=picklefiles
        self.xlsx=workname['xlsxname'] if 'xlsxname' in workname else '巡检报表'
        self.head=workname['head'] if 'head' in workname else None
        self.subhead=workname['subhead'] if 'subhead' in workname else None
        self.worksheet=workname['worksheet'] if 'worksheet' in workname else None
        self.systemtype=''


    def PointtoSet(self,point):
        # print(point)
        point_re=r'([A-Z0-9]{2,5})_(D|M)(\d+)_([A-Za-z0-9_]+)'
        res=re.findall(point_re,point)
        return res[0]
        # if type(point) is str and point.find('_')>0:
        #     res=point.split('_')
        #     res=(res[0],res[1][0],res[1][1],res[2])
        #     return res
        # else:
        #     return None,None,None,None


    def PointNameToStr(self,pointname):
        if pointname and type(pointname) is str:
            return str.strip(pointname)
        else:
            return ""

    def PointValueCal(self,
                      total_per,valid_per,
                      line_lenper,line_per,
                      exctime_lenper,exctime_per,
                      maxvalue,
                      over_lenper,over_per):
        res=[]
        """
        0总数据比例                  1 tv_per
        1有效数据比例                1 tv_per
        2最长直线跨度比例             1 lto_per
        3直线比例                    1
        4时间过长连续比例             1 lto_per
        5超过默认最长采点时间次数比例   1
        6最大值                      1
        7超限最长比例                 1
        8超限比例                    1  lto_per
        根据这个数据返回指标曲线是否正常
        ['极好','良好','正常','异常']
        ['部分直线','走直线','为零点过多','超限点过多','点间隔过长','有效点过少','点过少','W型异常点']
        """
        tv_per=total_per*valid_per
        lto_per=(1-line_lenper)*(1-over_per)*(1-exctime_lenper)
        all_per=tv_per*lto_per
        if all_per==1:
            res.append('极好')
        elif 0.8<=all_per<1:
            res.append('良好')
        elif 0.6<=all_per<0.8:
            res.append('正常')
        else:
            res.append('异常')
        #总点数
        if total_per<0.8:
            res.append('点过少')

        #零点
        if 0.8<=valid_per<=1:
            pass
        elif 0.5<=valid_per<0.8:
            pass
        elif valid_per<0.5:
            res.append('为零点过多')
        #点间隔
        if exctime_lenper<=0.2:
            pass
        elif exctime_lenper>0.2:
            res.append('点间隔过长')

        #直线
        if line_lenper<=0.2:
            pass
        elif line_lenper>0.2:
                res.append('走直线')
        #有效点
        if over_lenper<=0.2:
            pass
        elif 0.2<over_lenper<0.5:
            res.append('存在连线超限点')
        else:
            res.append('超限点过多')

        # print(res)
        return res

    def PickleWalk(self,picklefile):
        #添加系统类型判断
        if 'rdlc' in picklefile:
            self.systemtype='rdlc'
        elif 'reapuum' in picklefile:
            self.systemtype='reapuum'
        elif 'dnlms' in picklefile:
            self.systemtype='dnlms'
        elif 'dolms' in picklefile:
            self.systemtype='dolms'
        else:
            self.systemtype=''
        if os.path.isfile(picklefile):
            picklefile=picklefile
        elif os.path.isfile(os.path.join(os.path.dirname(__file__),picklefile)):
            picklefile=os.path.join(os.path.dirname(__file__),picklefile)
        f=open(picklefile,'rb')
        data=pickle.load(f)
        for item in data:
            if item[-1]:
                res=self.PointValueCal(*item[-1])
            else:
                if item[3]:
                    res=['采数异常']
                else:
                    res=['无数据']
            # print(item)
            #返回值结果如('新田电厂', 'XT_D1_fdjyggl', '#1机发电机有功功率','运行中', ['良好'])
            yield (item[0],self.PointNameToStr(item[2]),self.PointNameToStr(item[3]),item[4],res)

    def createxlsx(self):
        if not os.path.isdir(os.path.dirname(self.xlsx)):
            if os.path.isdir(os.path.dirname(__file__)):
                self.xlsx=os.path.join(os.path.dirname(__file__),self.xlsx)
            else:
                self.xlsx=os.path.join(os.getcwd(),self.xlsx)
        if not self.xlsx.endswith('.xlsx'):
            self.xlsx='{}.xlsx'.format(self.xlsx)
        tempfp,tempfile=mkstemp()
        wb=xlsxwriter.Workbook(tempfile)
        for picklefile in self.picklefiles:
        #循环各系统数据
            row,col=0,0
            wbsheet=os.path.abspath(picklefile).split('\\')[-1].split('.')[0]
            '''
            表格格式化
            1,都带有边框
            2,主、副标题 项目抬头字体使用加粗，上下居中对齐；主标题字号变大
            3,对巡检状态文字进行颜色标记
                极好          green
                良好          blue
                正常          silver
                异常          red
                无数据         orange
                采数异常       brown
            '''
            format_default=wb.add_format({'font_name':'微软雅黑','bottom':1,'top':1,'left':1,'right':1})
            #主标题格式
            format_head=wb.add_format({'font_name':'微软雅黑','font_size':'24',
                                       'align':'center','valign':'vcenter','bold':True,
                                       'bottom':1,'top':1,'left':1,'right':1})
            #副标题格式
            format_subhead=wb.add_format({'font_name':'微软雅黑','font_size':'14',
                                        'align':'center','valign':'vcenter','bold':True,
                                        'bottom':1,'top':1,'left':1,'right':1})
            #项目抬头格式
            format_project=wb.add_format({'font_name':'微软雅黑',
                                        'align':'center','valign':'center','bold':True,
                                        'bottom':1,'top':1,'left':1,'right':1})
            #合并单元格格式
            format_merge=wb.add_format({'align':'center','valign':'vcenter','font_name':'微软雅黑','bottom':1,'top':1,
                                        'left':1,'right':1})
            #巡检状态颜色标记
            format_green=wb.add_format({'font_color':'green','font_name':'微软雅黑','bottom':1,'top':1,'left':1,'right':1,'align':'center'})
            format_blue=wb.add_format({'font_color':'blue','font_name':'微软雅黑','bottom':1,'top':1,'left':1,'right':1,'align':'center'})
            format_silver=wb.add_format({'font_color':'silver','font_name':'微软雅黑','bottom':1,'top':1,'left':1,'right':1,'align':'center'})
            format_red=wb.add_format({'font_color':'red','font_name':'微软雅黑','bottom':1,'top':1,'left':1,'right':1,'align':'center'})
            format_orange=wb.add_format({'font_color':'orange','font_name':'微软雅黑','bottom':1,'top':1,'left':1,'right':1,'align':'center'})
            format_brown=wb.add_format({'font_color':'brown','font_name':'微软雅黑','bottom':1,'top':1,'left':1,'right':1,'align':'center'})
            format_dict={'极好':format_green,
                         '良好':format_blue,
                         '正常':format_silver,
                         '异常':format_red,
                         '无数据':format_orange,
                         '采数异常':format_brown,
                         '运行中':format_green,
                         '已停机':format_red,
                         '状态未知':format_silver}
            #worksheet表名
            if self.worksheet and wbsheet in self.worksheet:
                ws=wb.add_worksheet(self.worksheet[wbsheet])
            else:
                ws=wb.add_worksheet()
            #缩放比例
            ws.set_zoom(90)
            #设置行高列宽
            ws.set_row(0,50)
            ws.set_row(1,24)
            ws.set_column('A:A',15.5)
            ws.set_column('B:B',6)
            ws.set_column('C:C',4.63)
            ws.set_column('D:D',4.63)
            ws.set_column('E:E',12)
            ws.set_column('F:F',22.63)
            ws.set_column('G:G',33.38)
            ws.set_column('H:H',8.38)
            ws.set_column('I:I',8.38)
            ws.set_column('J:J',4)
            ws.set_column('K:K',15.5)
            ws.set_column('L:L',10.25)
            #ws.set_column('H:H',44.88)
            #窗口锁定
            ws.freeze_panes(3,0)
            #表格主标题
            if self.head and wbsheet in self.head:
                ws_head=self.head[wbsheet]
            else:
                ws_head='项目巡检表 主标题'
            ws.merge_range(row,row,0,8,ws_head,format_head)
            row+=1
            #表格副标题
            if self.subhead and wbsheet in self.subhead:
                ws_subhead=self.subhead[wbsheet]
            else:
                ws_subhead='项目巡检表 副标题'
            ws.merge_range(row,0,row,8,ws_subhead,format_subhead)
            row+=1
            #表格项目
            ws.write_row(row,0,['电厂','缩写','机组','类型','点类','点编号','点名称','曲线状态','机组状态'],format_project)
            row+=1
            #循环写入数据
            tempvalue=[]
            tempvalue_dict={}
            #电厂是否正常判断
            dcstate_dict={}
            #电厂顺序排序
            dc_list=[]
            #
            for item in self.PickleWalk(picklefile):
                #按顺序添加电厂
                if item[0] not in dc_list:
                    dc_list.append(item[0])
                # print(item)
                a,b,c,d=self.PointtoSet(item[1])
                row_res=[item[0],a,int(c),b,d,item[1],self.PointNameToStr(item[2])]
                # 写入行数据
                ws.write_row(row,0,row_res,format_default)
                ws.write(row,8,item[3],format_dict[item[3]])
                #写入巡检是否正常数据
                samevalue=''
                strvalue=''
                if item[-1]:
                    listvalue=item[-1]
                    samevalue={'极好','良好','正常','异常','无数据','采数异常'} & set(item[-1])
                    if samevalue:
                        #
                        samevalue=samevalue.pop()
                        #添加合并单元格颜色判断标记
                        # if samevalue in ['异常','无数据','采数异常']:
                        #     temp_merge[item[0]]=False
                        item[-1].remove(samevalue)
                        strvalue=','.join(listvalue)
                ws.write(row,7,samevalue,format_dict[samevalue])
                if strvalue:
                    ws.write_comment(row,7,strvalue)
                #判断电厂全厂当前检查点是否正常
                if item[0] not in dcstate_dict:
                    dcstate_dict[item[0]]=0
                if item[3] in {'已停机'}:
                    dcstate_dict[item[0]]+=1
                else:
                    if samevalue in {'极好','良好','正常'}:
                        dcstate_dict[item[0]]+=1
                #判断是否属于同一电厂，以便进行单元格合并
                tempvalue.append(item[0])
                row1=row-tempvalue.count(item[0])+1
                tempvalue_dict[item[0]]=(row1,0,row,0)

                #行+1
                row+=1
            else:
                # print(dc_list,dcstate_dict)
                # print(tempvalue,tempvalue_dict)
                dc_ratio=0.85
                #根据电厂类型，热电电厂正常性判断阀值降低
                if self.systemtype='rdlc':
                    dc_ratio -= 0.5
                if dc_list:
                    #ws.add_table(2,10,2+len(dc_list),11)
                    #ws.write_row(2,10,('电厂','总览'),format_project)
                    ws.write(2,11,'总览{}/{}'.format(len([k for k,v in dcstate_dict.items() if v/tempvalue.count(k) > dc_ratio]),len(dc_list)),format_project)
                    ws.write_url(2,10,'internal:{}!K4'.format(ws.get_name()),format_project,'电厂导航')
                num1=0
                for k in dc_list:
                    state_cal=dcstate_dict[k]/tempvalue.count(k)
                    if state_cal>dc_ratio:
                        # ws.write_row(3+num1,10,(k,'正常'),format_blue)
                        ws.write(3+num1,11,'正常',format_blue)
                        ws.write_url(3+num1,10,'internal:{}!A{}'.format(ws.get_name(),tempvalue.index(k)+4),
                                     format_blue,k)
                        #format_merge.set_bg_color('green')
                    else:
                        # ws.write(3+num1,10,(k,'异常'),format_orange)
                        ws.write(3+num1,11,'异常',format_orange)
                        ws.write_url(3+num1,10,'internal:{}!A{}'.format(ws.get_name(),tempvalue.index(k)+4),
                                     format_orange,k)
                        #format_merge.set_bg_color('orange')
                    ws.merge_range(*tempvalue_dict[k],data=k,cell_format=format_merge)
                    num1+=1

        else:
            wb.close()
            if os.path.exists(self.xlsx):
                self.xlsx='{}_{}.xlsx'.format(self.xlsx.rstrip('.xlsx'),time.strftime('_%H%M%S',
                                                time.localtime()))
            shutil.copy(tempfile,self.xlsx)
            os.close(tempfp)
            try:
                shutil.copy(self.xlsx,os.path.abspath(os.path.join(os.path.dirname(self.xlsx),'..')))
            except Exception:
                pass
            if os.path.isfile(self.xlsx):
                return self.xlsx
            else:
                return


if __name__=="__main__":
    test1=CreateXLSX(['reapuum.pickle','rdlc.pickle','dnlms.pickle','dolms.pickle'],
                     {'xlsxname':'巡检报表',
                      'head':{'reapuum':'煤耗巡检1'},
                      'subhead':{'reapuum':'煤耗巡检2'},
                      'worksheet':{'reapuum':'广东电网煤耗数据巡检','rdlc':'热电联产'}})
    # for x in test1.PickleWalk('reapuum.pickle'):
    #     print(x)
    test1.createxlsx()




