import json
import sys
import os
import time
import shutil
import traceback
import logging, logging.handlers

from email import encoders
from email.header import Header
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr

import smtplib


class JsonTransForm(object):
    """docstring for JsonTransForm"""

    def __init__(self):
        self.inputfilename=""
        self.destination_folder=""
        self.citynames=[]

    def filter_keys(self,obj):
        del obj['msg_type']
        del obj['udid']
        del obj['cid']
        del obj['country']
        del obj['tz']
        del obj['fea']
        del obj['BANGCLE_SDK_VERSION']
        del obj['mf_md5']
        del obj['sdk']
        del obj['content']['Rel']
        del obj['content']['ver']
        del obj['content']['SDK']
        return obj


    def filter_gps(self,obj):
        tmplist = obj['content']['data']

        if tmplist.get('low'):
            for  content in obj['content']['data']['low']:
                for  c in content:
                    if  c['gps']['status'] != "just":
                        # c.clear()
                        del c
        if tmplist.get('high'):
            for  content in obj['content']['data']['high']:
                for c in content:
                    if c['gps']['status'] != "just":
                        # c.clear()
                        del c
        return obj

    def write_csv_head(self,filename):
        head1="IMEI,IMSI,time,battery,gps.Lon,gps.Lat,speed.in,speed.out,ping.163,ping.baidu,ping.sina,ping.taobao,ping.qq,"
        head2="cell.Connected,cell.m,cell.networktype,cell.cell_type,cell.mnc,cell.tac(cid),cell.pci(lac),cell.sinr(dbm),cell.rsrp"
        fd = open(filename, 'a+',  encoding= 'utf-8')
        fd.write(head1+head2+'\n')
        return fd

    def get_csv_fds(self):
        fds={}
        path = self.destination_folder+'/'
        for name in self.citynames:
            fds[name]=self.write_csv_head(path+name+'.csv')
        return fds


    def splice_csv_content(self,obj, loworhigh):
        ret = []
        tmplist = obj['content']['data']
        if tmplist.get('low'):
            for  content in obj['content']['data'][loworhigh]:
                for  c in content:
                    glob = ""
                    glob+= str(obj['content']['IMEI'])+','
                    glob+= str(obj['content']['IMSI'])+','
                    glob+=c['time']+','

                    glob+=c['battery']+','
                    lon = str(c['gps'].get('Lon'))
                    lat = str(c['gps'].get('Lat'))
                    glob+=lon+','
                    glob+=lat+','
                    speedin = str(c['speed']['in'])
                    speedout =str(c['speed']['out'])
                    glob+=speedin+','
                    glob+=speedout+','

                    ping = str(c['ping']['163'])+','
                    ping += str(c['ping']['baidu'])+','
                    ping += str(c['ping']['sina'])+','
                    ping += str(c['ping']['taobao'])+','
                    ping += str(c['ping']['qq'])
                    glob += ping+','

                    for e in c['cell']:
                        cell_str = str(e.get('connected'))+','
                        cell_str += str(e.get('m'))+','
                        cell_str += str(e.get('networktype'))+','
                        cell_str += str(e.get('cell_type'))+','
                        cell_str += str(e.get('mnc'))+','
                        cell_str += str(e.get('cid'))+','
                        cell_str += str(e.get('lac'))+','
                        cell_str += str(e.get('dbm'))+','
                        cell_str += str(e.get('rsrp'))+','
                        ret.append(glob+cell_str)
        return ret

    def make_csv_content(self,obj):
        ret1 =[]
        ret2 =[]
        if obj['content']['data'].get('low'):
            ret1=self.splice_csv_content(obj, 'low')
        if obj['content']['data'].get('high'):
            ret2=self.splice_csv_content(obj, 'high')
        ret1.extend(ret2)
        return ret1


    def write_csv_content(self, fds, cityname, content):
        for x in content:
            fds[cityname].write(x)
            # f_csv_city1.write(x)
            fds[cityname].write('\n')


    def get_json_fds(self):
        fds={}
        path = self.destination_folder+'/'
        for name in self.citynames:
            fds[name]=open(path+name+'.json', 'a+',  encoding= 'utf-8')
        return fds

    def write_json_content(self, fds, obj):
        cityname=obj['city']
        fds[cityname].write(json.dumps(obj))

    def close_fds(self, fds):
        for x in fds:
            x.close()

    def create_destination_folder(self):
        basename = os.path.basename(self.inputfilename)
        #print(basename)
        str = os.getcwd()+'/'
        #print(str)
        #str='./'
        #str += os.path.splitext(self.inputfilename)[0]
        str += basename
        #print(str)
        #quit()
        if os.path.exists(str) == False:
            os.mkdir(str)
        else:
            shutil.rmtree(str)
            os.mkdir(str)
        # print(str)
        # quit()
        return str

class Logger():
    def __init__(self, logname, loglevel, logger):
        '''
           指定保存日志的文件路径，日志级别，以及调用文件
           将日志存入到指定的文件中
        '''

        # 创建一个logger
        self.logger = logging.getLogger(logger)
        self.logger.setLevel(logging.DEBUG)

        # 创建一个handler，用于写入日志文件
        fh = logging.FileHandler(logname)
        fh.setLevel(logging.NOTSET)

        # 定义handler的输出格式
        #formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        fh.setFormatter(formatter)

        # 给logger添加handler
        self.logger.addHandler(fh)

    def getlog(self):
        return self.logger

class Send_mail(object):

    def __init__(self):
        self.from_addr = 'king.ye@blueforce-tech.com'
        self.password = 'QQdemima1'
        self.to_addr = 'bruce.deng@blueforce-tech.com'
        self.smtp_server = 'smtp.qq.com'
        self.content=""
        self.msg=""

    def _format_addr(self, s):
        name, addr = parseaddr(s)
        return formataddr((Header(name, 'utf-8').encode(), addr))


    def create_mail(self, content):
        self.msg = MIMEText(content, 'plain', 'utf-8')
        self.msg['From'] = self._format_addr('JsonTransForm <%s>' % self.from_addr)
        self.msg['To'] = self._format_addr('Roboot <%s>' % self.to_addr)
        self.msg['Subject'] = Header('Something happendd', 'utf-8').encode()

    def send_email(self):
        server = smtplib.SMTP(self.smtp_server, 25)
        server.set_debuglevel(1)
        server.login(self.from_addr, self.password)
        server.sendmail(self.from_addr, [self.to_addr], self.msg.as_string())
        server.quit()



names=[
'中国_广东_广州','中国_湖北_武汉','中国_四川_成都','中国_云南_昆明','中国_甘肃_兰州','中国_台湾_台北','中国_广西_南宁','中国_宁夏_银川','中国_山西_太原','中国_吉林_长春','中国_江苏_南京'
]


def do_once(filenamein, names):
    tool=JsonTransForm()
    tool.inputfilename=filenamein#初始化想要处理的文件名
    tool.destination_folder = tool.create_destination_folder()#初始化目标文件夹
    tool.citynames=names
    fds_json=tool.get_json_fds()
    fds_csv=tool.get_csv_fds()
    try :
        with open(filenamein, 'r',  encoding= 'utf-8') as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    obj = tool.filter_keys(obj)
                    tool.write_json_content(fds_json,obj)
                    content=tool.make_csv_content(obj)
                    tool.write_csv_content(fds_csv,obj['city'], content)
                except  :
                    continue
    except Exception as ex:
        content=str(ex)+'\nFilename is: '+filenamein+'\n'
        logger = Logger(logname=tool.destination_folder+'/LOG.txt', loglevel=1, logger='ex').getlog().error(content)
        a=Send_mail()
        a.create_mail(content)
        a.send_email()
        exit(213)
    logger = Logger(logname=tool.destination_folder+'/LOG.txt', loglevel=1, logger='ex').getlog().info('FILE: <'+filenamein+'> SUCCESS')
    # tool.close_fds(fds_csv)
    # tool.close_fds(fds_json)


filenamein = sys.argv[1]
do_once(filenamein,names)






