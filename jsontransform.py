import json
import sys
import os
import time, datetime
import shutil
import traceback
import logging, logging.handlers
import encodings.idna

from email import encoders
from email.header import Header
from email.mime.text import MIMEText
from email.utils import parseaddr, formataddr
import smtplib
import threading


class JsonTransForm(object):
    """docstring for JsonTransForm"""

    #初始化成员变量
    def __init__(self):
        self.inputfilename=""#要解析的文件名
        self.destination_folder=""#生成文件的路径
        self.citynames=[]#要解析的城市名
        self.starttime = datetime.datetime.now()#程序开始的时间
        self.endtime=""#程序结束的时间

    #创建输出的目标路径
    def create_destination_folder(self):
        basename = os.path.basename(self.inputfilename)[0:-4]
        str = os.getcwd()+'/'
        str += basename
        if os.path.exists(str) == False:
            os.mkdir(str)
        else:
            shutil.rmtree(str)
            os.mkdir(str)
        return str


    #过滤json数据的一些键，目前是写死的， 如有更改需要手动改动
    #obj：传入一个dict
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


    #过滤无效的gps（目前没有使用此函数）
    #obj：传入一个dict
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

    #生成csv的文件头
    #filename：生成的文件名
    def write_csv_head(self,filename):
        head1="IMEI,IMSI,time,battery,gps.Lon,gps.Lat,speed.in,speed.out,ping.163,ping.baidu,ping.sina,ping.taobao,ping.qq,"
        head2="cell.Connected,cell.m,cell.networktype,cell.cell_type,cell.mnc,cell.tac(cid),cell.pci(lac),cell.sinr(dbm),cell.rsrp"
        fd = open(filename, 'a+',  encoding= 'utf-8')
        fd.write(head1+head2+'\n')
        return fd

    #获取生成的csv文件的文件句柄
    def get_csv_fds(self):
        fds={}
        path = self.destination_folder+'/'
        for name in self.citynames:
            fds[name]=self.write_csv_head(path+name+'.csv')
        return fds

    #下面三个函数是拼接csv文件内容并写入到文件中
    #obj：传入一个dict
    #loworhigh：一共两种情况’low‘， ’high‘
    def splice_csv_content(self, obj, loworhigh):
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

    #生成完整的csv行数据内容
    #obj：传入一个dict
    def make_csv_content(self, obj):
        ret1 =[]
        ret2 =[]
        if obj['content']['data'].get('low'):
            ret1=self.splice_csv_content(obj, 'low')
        if obj['content']['data'].get('high'):
            ret2=self.splice_csv_content(obj, 'high')
        ret1.extend(ret2)
        return ret1

    #写入csv文件内容
    #fds：文件句柄
    #cityname：城市名
    #content：内容
    def write_csv_content(self, fds, cityname, content):
        for x in content:
            fds[cityname].write(x)
            # f_csv_city1.write(x)
            fds[cityname].write('\n')

    #获取生成的json文件的文件句柄
    def get_json_fds(self):
        fds={}
        path = self.destination_folder+'/'
        for name in self.citynames:
            fds[name]=open(path+name+'.json', 'a+',  encoding= 'utf-8')
        return fds
    #写入json文件内容
    #fds：文件句柄
    #obj：传入一个dict
    def write_json_content(self, fds, obj):
        cityname=obj['city']
        fds[cityname].write(json.dumps(obj))

    #关闭文件句柄（貌似是多余的）
    def close_fds(self, fds):
        for x in fds:
            x.close()


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
    def __init__(self, config):
        self.from_addr = config['from_addr']#发送者的邮箱
        self.password = config['password']#发送者邮箱密码
        self.to_addr = config['to_addr']#接收者的邮箱
        self.smtp_server = config['smtp_server']#邮箱服务器
        # self.smtp_port = config['smtp_port']##邮箱服务器端口
        # print(config)
        # quit()

        self.content=""
        self.msg=""

    def _format_addr(self, s):
        name, addr = parseaddr(s)
        return formataddr((Header(name, 'utf-8').encode(), addr))


    def create_mail(self, content):
        self.msg = MIMEText(content, 'plain', 'utf-8')
        self.msg['From'] = self._format_addr('JsonTransFormTool <%s>' % self.from_addr)
        self.msg['To'] = self._format_addr('Roboot <%s>' % self.to_addr)
        self.msg['Subject'] = Header('Something happendd', 'utf-8').encode()

    def send_email(self):
        server = smtplib.SMTP(self.smtp_server, 25)
        server.set_debuglevel(1)
        server.login(self.from_addr, self.password)
        server.sendmail(self.from_addr, [self.to_addr], self.msg.as_string())
        server.quit()


#获取配置文件
def get_config(config_file):
    try:
        with open(config_file, 'r', encoding='utf-8') as f:
            config_str=f.read()
            config_dict = json.loads((config_str))
            return config_dict
    except Exception as e:
        print("打开配置文件失败: "+str(e))

#执行一次文件解析
#log_file： 输入的文件名
#names： 解析的城市名
def do_once(log_file, config_file):
    config=get_config(config_file)
    if config == None:
        quit()
    else:
        tool=JsonTransForm()
        tool.inputfilename=log_file#初始化想要处理的文件名
        tool.destination_folder = tool.create_destination_folder()#初始化目标文件夹
        tool.citynames=config['citys']['names']
        fds_json=tool.get_json_fds()#获取json文件句柄
        fds_csv=tool.get_csv_fds()#获取csv文件句柄
        try :
            with open(log_file, 'r',  encoding= 'utf-8') as f:
                for line in f:
                    try:
                        obj = json.loads(line)
                        obj = tool.filter_keys(obj)
                        tool.write_json_content(fds_json,obj)
                        content=tool.make_csv_content(obj)
                        tool.write_csv_content(fds_csv,obj['city'], content)
                    except  :#loads行数据时抛出异常，则直接丢弃该行数据
                        continue
        except Exception as ex:#打开log文件抛出的一场
            content=str(ex)+'\nFilename is: '+log_file+'\n'
            logger = Logger(logname=tool.destination_folder+'/LOG.txt', loglevel=1, logger='ex').getlog().error(content)
            a=Send_mail(config['email'])
            a.create_mail(content)
            a.send_email()
            exit(213)

        tool.endtime = datetime.datetime.now()
        total_seconds=(tool.endtime - tool.starttime).total_seconds()
        # time_cost_str= '\n一共耗时: '+str(total_seconds//60)+' 分 '+str(total_seconds%60)+' 秒 '
        content="文件解析成功  "
        a=Send_mail(config['email'])
        a.create_mail(content)
        a.send_email()
        logger = Logger(logname=tool.destination_folder+'/LOG.txt', loglevel=1, logger='ex').getlog().info('FILE: <'+log_file+'> SUCCESS')



# argvs=[]
# for i in range(1,len(sys.argv)):
#     argvs.append(sys.argv[i])

# # 这是多线程版本的程序， 程序运行时传入所有的log文件即可
# threads = []

# for i in argvs:
#     threads.append(threading.Thread(target=do_once,args=(i, names,)))

# for t in threads:
#     t.setDaemon(True)
#     t.start()

# t.join()

# config_file=sys.argv[2]
# config=get_config(config_file)
# a=Send_mail(config['email'])
# a.create_mail(content)
# a.send_email()
# quit()

if len(sys.argv) != 3:
    print("Usage: jsontransform <log_file> <config_file>")
else:
    log_file=sys.argv[1]
    config_file=sys.argv[2]
    do_once(log_file, config_file)



