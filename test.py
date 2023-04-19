import os
import read_bin
import write2arv
import time
import configparser
import threading
import datetime

from pathlib import Path
from watchdog.observers import Observer
from watchdog.events import *

#读取路径信息
LOCATION = ['南昌','九江','上饶','抚州','宜春','吉安','赣州',
         '景德镇','武汉','岳阳','长沙','衡阳','郴州','韶关',
         '梅州','汕头','安庆','黄山','铜陵','三明','衢州']

path_io = os.path.join(os.getcwd(),'io.ini')      #设置ini配置文件路径
config = configparser.ConfigParser() # 类实例化
config.read(path_io)
PATH_INPUT = config['io']['PATH_INPUT']
PATH_OUTPUT = config['io']['PATH_OUTPUT']

location_list = [os.path.join(PATH_INPUT,i) for i in LOCATION]
print(location_list)