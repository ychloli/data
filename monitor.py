
import os
import read_bin
import write2arv
import time
import configparser
import threading
import datetime
import sys

from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import *

#读取路径信息,全局变量
LOCATION = ['南昌','九江','上饶','抚州','宜春','吉安','赣州',
         '景德镇','武汉','岳阳','长沙','衡阳','郴州','韶关',
         '梅州','汕头','安庆','黄山','铜陵','三明','衢州']

path_io = os.path.join(os.getcwd(),'io.ini')      #设置ini配置文件路径
if not Path(path_io).exists():
    print('错误:.ini配置文件未与.exe在同一路径下\n')
    input('输入任意值退出')
    sys.exit(1)
config = configparser.ConfigParser() # 类实例化
config.read(path_io)
PATH_INPUT = config['io']['PATH_INPUT']
PATH_OUTPUT = config['io']['PATH_OUTPUT']
DELTA_TIME = int(config['del']['DELTA_TIME'])
SLEEP_TIME = int(config['del']['SLEEP_TIME'])
if not os.path.exists(PATH_INPUT):
    print('错误:原始数据文件夹不存在,请检查.ini配置文件路径是否正确\n')
    input('输入任意值退出')
    sys.exit(1)

location_list = [os.path.join(PATH_INPUT,i) for i in LOCATION]  #提前获取列表,只计算input文件夹的子一级文件夹内新增的数据

#文件夹监测模块
class MyHandler(FileSystemEventHandler):
    def __init__(self):
        FileSystemEventHandler.__init__(self)
         
    def on_created(self,event):
        global error_flag
        error_flag = 0
        try:     
            print('寻找文件')
            print(event.src_path)
            if event.src_path[-4:]=='.bz2':
                if os.path.dirname(event.src_path) not in location_list:
                    print('Thread-1:发现新数据文件,3S后开始转化\n',event.src_path,'\n')
                    time.sleep(3)     #等待几秒,等bin数据完全写入后再读,否则读取容易出错
                    generic,site,task,cut,radial = read_bin.read_from_bin(event.src_path) #数据读取
                    #信息转换
                    location,volumn_header,message,site_code = write2arv.change2arv(generic,site,task,cut,radial)
                    #输出至.ar2v
                    write2arv.write2arv(volumn_header,message,PATH_OUTPUT,location,site_code)    
        except Exception as e:
            with open(os.path.join(os.getcwd(),'error.txt'), 'a+') as f_error:
                f_error.write(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())+'\n')
                f_error.write(repr(e)+'\n')
            print('Thread-1:转换程序出错,等待重新启动\n')
            error_flag = 1
            
#线程1,转换程序           
def auto_change():
    #主程序
    while True:
        global error_flag
        error_flag = 0
        #try:
        print('Thread-1:开始监控原始数据文件夹是否有新数据传入\n')                
        #监控目标文件夹及其子文件夹是否有bz2文件生成
        observer = Observer()
        #for PATH_INPUT in dirs:    #监控多个文件夹         
        event_handler = MyHandler()
        observer.schedule(event_handler, PATH_INPUT, recursive=True)   
        observer.start()
        while True:
            if error_flag == 1:
                break
            else:
                print('error_flag == 0')
                
                time.sleep(1)
                break
        if error_flag == 1:
            observer.stop()
            observer.join()
            continue 
        print(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
        
   
#线程2，自动删除久远文件部分
def auto_del():
    while True:
        try:
            print('Thread-2:删除程序启动\n')
            today = datetime.datetime.now()   #获取当前
            delta_time = datetime.timedelta(DELTA_TIME)    #获取时间间隔
            yuzhi = today-delta_time                  
            for root, dirs, files in os.walk(PATH_OUTPUT):
                for name in files:
                    if name[-5:] == '.ar2v':
                        name_year = int(name[5:9])
                        name_month = int(name[9:11])
                        name_date = int(name[11:13])
                        name_hour = int(name[14:16])
                        name_minute = int(name[16:18])
                        name_second = int(name[18:20])
                        name_time = datetime.datetime(name_year,name_month,name_date,name_hour,name_minute,name_second)
                        name_time = name_time+datetime.timedelta(hours = 8)    #转化为北京时要+8
                        if name_time<yuzhi:
                            path_del = os.path.join(root, name)
                            print('Thread-2:发现历史文件,正在删除:\n',path_del,'\n')
                            #time.sleep(5)
                            os.remove(path_del)
                            print('Thread-2:删除成功:\n',path_del,'\n')
            print('Thread-2:删除任务完成, %d 秒后重新运行\n' %(SLEEP_TIME))
            time.sleep(SLEEP_TIME)                #删除程序运行周期定为30分钟,单位为s
        except:
            print('Thread-2:自动删除程序出错,5s后重新启动\n')
            time.sleep(5)  


auto_change()

#双线程并行
# threads = []
# t1 = threading.Thread(target=auto_change)
# threads.append(t1)
# t2 = threading.Thread(target=auto_del)
# threads.append(t2)

# if __name__ == '__main__':
#     for t in threads:
#         t.start()
#     for t in threads:
#         t.join()