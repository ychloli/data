'''
功能：将标准格式的雷达数据文件转化成gr2能读的WSR-88D格式的.gr2v文件(存储类型为type31)
使用方法：将.bz2基数据文件放入input文件夹下，修改程序中文件夹对应的路径，程序会把该文件夹下的所有.bin
        都转换成.ar2v并保存至output文件内，为避免重复计算，请运行后清空input文件夹     
作者: 卢文旭
'''
import os
import read_bin
import write2arv

#从目标文件获取需要转换的数据名称列表
#!!!需要修改的部分
PATH = r'E:\radar'     #bin2arv文件夹所在路径

PATH_INPUT = PATH+r'\input'
LIST_INPUT = os.listdir(PATH_INPUT)

#输出目录
PATH_OUTPUT = PATH+r'\output'
#转换开始
for name in LIST_INPUT:
    #读取bin文件标准格式雷达数据
    if name[-4:]=='.bz2':
        PATH_READ = PATH_INPUT+'\\'+name
            
        generic,site,task,cut,radial = read_bin.read_from_bin(PATH_READ) #数据读取
        #信息转换
        location,volumn_header,message,site_code = write2arv.change2arv(generic,site,task,cut,radial)
        #输出至.ar2v
        write2arv.write2arv(volumn_header,message,PATH_OUTPUT,location,site_code) 

#print('转换结束，请删除input文件夹内文件')