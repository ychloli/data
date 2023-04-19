
import struct
import numpy as np
import bz2

UINT4 = 'I'
INT4 = 'i'
USHORT2 = 'H'
SHORT2 = 'h'
FLOAT4 = 'f'
LONG8 = 'd' 

GENERIC_HEADER = (
    ('magic_number',INT4),
    ('Major_version',USHORT2),
    ('Minor_version',USHORT2),
    ('Generic_type',INT4),
    ('Product_type',INT4),
    ('Reserved','16s'),
)                              #通用头文件32个字节

SITE_HEADER = (
    ('Site_code','8s'),
    ('Site_name','32s'),
    ('Latitude',FLOAT4),
    ('Longitude',FLOAT4),
    ('Antenna_h',INT4),
    ('Grand_h',INT4),
    ('Frequency',FLOAT4),
    ('Beam_width_hori',FLOAT4),
    ('Beam_width_vert',FLOAT4),
    ('RDA_version',INT4),
    ('Radar_type',SHORT2),
    ('Antenna_Gain',SHORT2),
    ('Transmitting_feeder_loss',SHORT2),
    ('Receiving_feeder_loss',SHORT2),
    ('Other_loss',SHORT2),
    ('Reserved2','46s'),
)                               #站点配置块128个字节

TASK_HEADER = (
    ('Task_name','32s'),
    ('Task_description','128s'),
    ('Polar_type',INT4),
    ('Scan_type',INT4),
    ('Pulse_width',INT4),
    ('Scan_start_time',INT4),
    ('Cut_number',INT4),
    ('Horizontal_noise',FLOAT4),
    ('Vertical_noise',FLOAT4),
    ('Horizontal_calibration',FLOAT4),
    ('Vertical_calibration',FLOAT4),
    ('Horizontal_noise_temperature',FLOAT4),
    ('Vertical_noise_temperature',FLOAT4),
    ('ZDR_calibration',FLOAT4),
    ('PHI_calibration',FLOAT4),
    ('LDR_calibration',FLOAT4),
    ('Reserved','40s'),
)                              #任务配置块共256个字节

CUT_HEADER = (
    ('Process_mode',INT4),
    ('Wave_form',INT4),
    ('PRF1',FLOAT4),
    ('PRF2',FLOAT4),
    ('Dealiasing_mode',INT4),
    ('Azimuth',FLOAT4),
    ('Elevation',FLOAT4),
    ('Start_angle',FLOAT4),
    ('End_angle',FLOAT4),
    ('Angular_resolution',FLOAT4),
    ('Scan_speed',FLOAT4),
    ('Log_resolution',INT4),
    ('Doppler_resolution',INT4),
    ('Maximum_range1',INT4),
    ('Maximum_range2',INT4),
    ('Start_range',INT4),
    ('Sample1',INT4),
    ('Sample2',INT4),
    ('Phase_mode',INT4),
    ('Atmos_loss',FLOAT4),
    ('Nyquist_speed',FLOAT4),
    ('Moments_Mask',LONG8),
    ('Moments_Size',LONG8),
    ('Misc_filter_mask',INT4),
    ('SQI_threshold',FLOAT4),
    ('SIG_threshold',FLOAT4),
    ('CSR_threshold',FLOAT4),
    ('LOG_threshold',FLOAT4),
    ('CPA_threshold',FLOAT4),
    ('PMI_threshold',FLOAT4),
    ('DPLOG_threshold',FLOAT4),
    ('Threshold_reserved','4s'),
    ('DBT_mask',INT4),
    ('DBZ_mask',INT4),
    ('Velocity_mask',INT4),
    ('Spectrum_mask',INT4),
    ('DP_mask',INT4),
    ('Mask_reserved','12s'),
    ('Reserved1','4s'),
    ('Direction',INT4),
    ('Groud_clutter_classifier_type',SHORT2),
    ('Groud_clutter_filter_type',SHORT2),
    ('Groud_clutter_filter_notch_width',SHORT2),
    ('Groud_clutter_filter_window',SHORT2),
    ('Reserved2','72s'),
)                                     #扫描配置块共256个字节

RADIAL_HEADER = (
    ('Radial_State',INT4),
    ('Spot_Blank',INT4),
    ('Sequence_number',INT4),
    ('Radial_number',INT4),
    ('Elevation_number',INT4),
    ('Azimuth',FLOAT4),
    ('Elevation',FLOAT4),
    ('Seconds',INT4),
    ('Microseconds',INT4),
    ('Length_of_data',INT4),
    ('Moment_number',INT4),
    ('Reserved1','2s'),
    ('Horizontal_estimated_noise',SHORT2),
    ('Vertical_estimated_noise',SHORT2),
    ('Reserved2','14s'),
)                                   #径向头共64字节

MOMENT_HEADER = (
    ('Data_type',INT4),
    ('Scale',INT4),
    ('Offset',INT4),
    ('Bin_length',SHORT2),
    ('Flags',SHORT2),
    ('Length',INT4),
    ('Reserved','12s'),
)                                   #径向数据头共32字节

DATA_MOMENT_TYPE = (
    (0,'Reserved1'),(1,'dBT'),(2,'dBZ'),(3,'V'),(4,'SW'),(5,'SQI'),
    (6,'CPA'),(7,'ZDR'),(8,'LDR'),(9,'CC'),(10,'FDP'),(11,'KDP'),
    (12,'CP'),(13,'Reserved2'),(14,'HCL'),(15,'CF'),(16,'SNRH'),(17,'SNRV'),
    (18,'Reserved3'),(19,'POTS'),(20,'Reserved4'),(21,'COP'),(22,'Reserved5'),(23,'Reserved6'),
    (24,'Reserved7'),(25,'Reserved8'),(26,'VELSZ'),(27,'DR'),(28,'Reserved9'),(29,'Reserved10'),
    (30,'Reserved11'),(31,'Reserved12'),(32,'ZC'),(33,'VC'),(34,'WC'),(35,'ZDRC'),
)                                #数据类型

def _structure_size(structure):
    """ Find the size of a structure in bytes. """
    return struct.calcsize('<' + ''.join([i[1] for i in structure]))

def _unpack_from_buf(buf, pos, structure):
    """ Unpack a structure from a buffer. """
    size = _structure_size(structure)
    return _unpack_structure(buf[pos:pos + size], structure)

def _unpack_structure(string, structure):
    """ Unpack a structure from a string. """
    fmt = '<' + ''.join([i[1] for i in structure])  # NEXRAD is big-endian
    lst = struct.unpack(fmt, string)
    return dict(zip([i[0] for i in structure], lst))

def read_from_bin(path):
    #开始读取   
    with bz2.open(path,'rb') as f:       #读进来的数据是.bz2压缩文件,需要用bz2库去读取
        buf = f.read()
        buf_len = len(buf)
        pos = 0             #起始位置序号
        
        #读取通用头文件
        generic = _unpack_from_buf(buf,pos,GENERIC_HEADER)
        pos = pos+_structure_size(GENERIC_HEADER)
        #读取站点配置块
        site = _unpack_from_buf(buf,pos,SITE_HEADER)
        pos = pos+_structure_size(SITE_HEADER)
        #读取任务配置块
        task = _unpack_from_buf(buf,pos,TASK_HEADER)
        pos = pos+_structure_size(TASK_HEADER)
        #读取扫描配置块
        cut = []
        for i in np.arange(0,task['Cut_number']):
            cut.append(_unpack_from_buf(buf,pos,CUT_HEADER))
            pos = pos+_structure_size(CUT_HEADER)
        
        #读取体扫数据
        radial = []      #每次射线记为一个字典temp
        while pos<buf_len:
            temp = {}
            radial_header = _unpack_from_buf(buf,pos,RADIAL_HEADER)     #读取径向总头,每个代表接收一次射线
            pos = pos+_structure_size(RADIAL_HEADER)   #位置更新
            temp.update({'radial_header' : radial_header})     #在temp字典中增加径向总头文件信息        
            moment = {}    #数据头初始化           
            for i in np.arange(radial_header['Moment_number']):      #径向数据头数目
                moment = _unpack_from_buf(buf,pos,MOMENT_HEADER)     #读取径向数据头信息
                #ts = radial_header['Radial_State']         #扫描状态更新,如果为4,则扫描结束,下次循环结束
                pos = pos+_structure_size(MOMENT_HEADER)             #位置更新
                size_moment = moment['Length']
                if moment['Length']%2 != 0:                           #这个值对应了ngates这个变量,表示该数据的个数,检验证明这个值必须是偶数
                    moment['Length'] = moment['Length']-1             #奇数就要减一，不然gr2识别不了该仰角的数据,不要问我为什么,我也不知道！
                if moment['Bin_length']==1:                           #如果每个数据存储占一个字节
                    len_data = str(int(moment['Length']))
                    data = list(struct.unpack('<'+len_data+'B',buf[pos:pos+moment['Length']]))
                elif moment['Bin_length']==2:                       #如果每个数据存储占两个字节
                    len_data = str(int(moment['Length']/2))
                    data = list(struct.unpack('<'+len_data+'H',buf[pos:pos+moment['Length']]))  
                pos = pos + size_moment                       #位置更新   
                moment.update({'data': data})                        #将解码的data也保存至moment内
                moment_name = DATA_MOMENT_TYPE[moment['Data_type']][1]   #获取当前moment的数据类型为str
                temp.update({moment_name : moment})         #为temp字典添加此次径向数据头信息和提取的数据
            radial.append(temp)                       # 将temp字典存入radial数组中      

    print('Thread-1:成功读取文件\n',path,'\n')
    return generic,site,task,cut,radial