
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