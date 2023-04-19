'''
1.将读出的bin雷达信息转换成message31的格式
2.将message31写入arv2格式的雷达数据
(message type31格式基于2620002N文档
 volume header基于2620010E文档)
作者: 卢文旭
'''
import os
import struct
import datetime as dt
import numpy as np

CODE1 = 'B'
CODE2 = 'H'
INT1 = 'B'
INT2 = 'H'
INT4 = 'I'
REAL4 = 'f'
REAL8 = 'd'
SINT1 = 'b'
SINT2 = 'h'
SINT4 = 'i'

# Figure 1 in Interface Control Document for the Archive II/User
# page 7-2
VOLUME_HEADER = (
    ('tape', '9s'),
    ('extention', '3s'),
    ('date', 'I'),
    ('time', 'I'),
    ('icao', '4s'),
)

# Table II Message Header Data
# page 3-7
MSG_HEADER = (
    ('size', INT2),                 # size of data, no including header
    ('channels', INT1),
    ('type', INT1),
    ('seq_id', INT2),
    ('date', INT2),
    ('ms', INT4),
    ('segments', INT2),
    ('seg_num', INT2),
)

# Table XVII Digital Radar Generic Format Blocks (Message Type 31)
# pages 3-87 to 3-89
MSG_31 = (
    ('id', '4s'),                   # 0-3
    ('collect_ms', INT4),           # 4-7
    ('collect_date', INT2),         # 8-9
    ('azimuth_number', INT2),       # 10-11
    ('azimuth_angle', REAL4),       # 12-15
    ('compress_flag', CODE1),       # 16
    ('spare_0', INT1),              # 17
    ('radial_length', INT2),        # 18-19
    ('azimuth_resolution', CODE1),  # 20
    ('radial_status', CODE1),      # 21
    ('elevation_number', INT1),     # 22
    ('cut_sector', INT1),           # 23
    ('elevation_angle', REAL4),     # 24-27
    ('radial_blanking', CODE1),     # 28
    ('azimuth_mode', SINT1),        # 29
    ('block_count', INT2),          # 30-31
    ('block_vol', INT4),      # 32-35  Volume Data Constant XVII-E
    ('block_elv', INT4),      # 36-39  Elevation Data Constant XVII-F
    ('block_rad', INT4),      # 40-43  Radial Data Constant XVII-H
    ('block_ref', INT4),      # 44-47  Moment "REF" XVII-{B/I}
    ('block_vel', INT4),      # 48-51  Moment "VEL"
    ('block_sw', INT4),      # 52-55  Moment "SW"
    ('block_zdr', INT4),      # 56-59  Moment "ZDR"
    ('block_phi', INT4),      # 60-63  Moment "PHI"
    ('block_rho', INT4),      # 64-67  Moment "RHO"
)

# Table XVII-B Data Block (Descriptor of Generic Data Moment Type)
# pages 3-90 and 3-91
GENERIC_DATA_BLOCK = (
    ('block_type', '1s'),
    ('data_name', '3s'),        # VEL, REF, SW, RHO, PHI, ZDR
    ('reserved', INT4),
    ('ngates', INT2),
    ('first_gate', SINT2),
    ('gate_spacing', SINT2),
    ('thresh', SINT2),
    ('snr_thres', SINT2),
    ('flags', CODE1),
    ('word_size', INT1),
    ('scale', REAL4),
    ('offset', REAL4),
    # then data
)

# Table XVII-E Data Block (Volume Data Constant Type)
# page 3-92
VOLUME_DATA_BLOCK = (
    ('block_type', '1s'),
    ('data_name', '3s'),
    ('lrtup', INT2),
    ('version_major', INT1),
    ('version_minor', INT1),
    ('lat', REAL4),
    ('lon', REAL4),
    ('height', SINT2),
    ('feedhorn_height', INT2),
    ('refl_calib', REAL4),
    ('power_h', REAL4),
    ('power_v', REAL4),
    ('diff_refl_calib', REAL4),
    ('init_phase', REAL4),
    ('vcp', INT2),
#    ('spare0',INT1),
    ('spare', '2s'),
)

# Table XVII-F Data Block (Elevation Data Constant Type)
# page 3-93
ELEVATION_DATA_BLOCK = (
    ('block_type', '1s'),
    ('data_name', '3s'),
    ('lrtup', INT2),
    ('atmos', SINT2),
    ('refl_calib', REAL4),
)

# Table XVII-H Data Block (Radial Data Constant Type)
# pages 3-93
RADIAL_DATA_BLOCK = (
    ('block_type', '1s'),
    ('data_name', '3s'),
    ('lrtup', INT2),
    ('unambig_range', SINT2),
    ('noise_h', REAL4),
    ('noise_v', REAL4),
    ('nyquist_vel', SINT2),
    ('spare','2s'),
    ('horizontal_channel',REAL4),
    ('vertical_channel',REAL4),
)

#站点信息配置
LOCATION1 = {'Z9791':'NCJX','Z9792':'JJJX','Z9793':'SRJX','Z9794':'FZJX','Z9795':'YCJX','Z9796':'JAJX',
             'Z9797':'GZJX','Z9798':'JDZR','Z9270':'WHHB','Z9730':'YYHN','Z9731':'CSHN','Z9734':'HYHN',
             'Z9735':'CZHN','Z9751':'SGGD','Z9753':'MZGD','Z9754':'STGD','Z9556':'AQAH','Z9559':'HSAH',
             'Z9562':'TLAH','Z9598':'SMFJ','Z9570':'QZZJ'}

LOCATION2 = {'Z9791':'南昌','Z9792':'九江','Z9793':'上饶','Z9794':'抚州','Z9795':'宜春','Z9796':'吉安',
             'Z9797':'赣州','Z9798':'景德镇','Z9270':'武汉','Z9730':'岳阳','Z9731':'长沙','Z9734':'衡阳',
             'Z9735':'郴州','Z9751':'韶关','Z9753':'梅州','Z9754':'汕头','Z9556':'安庆','Z9559':'黄山',
             'Z9562':'铜陵','Z9598':'三明','Z9570':'衢州'}

def _structure_size(structure):
    """ Find the size of a structure in bytes. """
    return struct.calcsize('>' + ''.join([i[1] for i in structure]))

def _pack_structure(f,structure,data):
    '''
    Parameters
    ----------
    f : 写入的文件头
    structure : 列表，头文件的信息
    data : 字典格式，转换后的数据
    Returns
    -------
    '''
    for i in np.arange(0,len(structure)):
        fmt = '>'+ structure[i][1]
        string = data[structure[i][0]]
        if type(string)==type('str'):
            byte = struct.pack(fmt,bytes(string.encode('utf-8')))
        else:
            byte = struct.pack(fmt,string)
        f.write(byte)

def change2arv(generic,site,task,cut,radial):
    #获取站点信息
    site_code = site['Site_code'][0:5].decode('utf-8')
    location = LOCATION1[site_code]  
    #获取VCP信息
    vcp = int(task['Task_name'][3:5].decode('utf-8'))
    #转换至volumn头文件信息
    volumn = {}
    volumn.update({'tape':'AR2V0006.','extention':'001','icao':location})
    delta = dt.timedelta(0,task['Scan_start_time'])
    volumn.update({'date':delta.days+1,'time':delta.seconds*1000})
    #radial信息转换至message信息
    message = []   #初始化,长度与radial一致,每个射线存放其对应的字典
    for i in np.arange(0,len(radial)):
        temp = {}     #字典信息
        sum_bit = 0    #用来计算头文件中开始的字节位置
        block_count = 0  #用来计数有多少个block
        j = int(radial[i]['radial_header']['Elevation_number']-1)     #得到该射线下对应cut中第几个仰角信息
        
        #写入message头信息
        message_header = {}
        delta = dt.timedelta(0,radial[i]['radial_header']['Seconds'])
        message_header.update({'date':delta.days+1,'ms':delta.seconds*1000,'type':31})
        message_header.update({'channels':8,'seg_num':1,'segments':1,'seq_id':1})  #这项不确定,原数据找不到,只能和例子中的数据一致
        #还有size没输入,最后输入
        
        #写入message31头信息
        message31_header = {}
        message31_header.update({'id':location,'collect_date':delta.days+1,'collect_ms':delta.seconds*1000})
        message31_header.update({'azimuth_number':radial[i]['radial_header']['Radial_number']})
        message31_header.update({'azimuth_angle':radial[i]['radial_header']['Azimuth']})
        message31_header.update({'compress_flag':0,'spare_0':0})
        if cut[j]['Angular_resolution']==1.0:
            message31_header.update({'azimuth_resolution':2})
        else:
            message31_header.update({'azimuth_resolution':1})
        message31_header.update({'radial_status':radial[i]['radial_header']['Radial_State']})
        message31_header.update({'elevation_number':radial[i]['radial_header']['Elevation_number']})
        message31_header.update({'cut_sector':1})
        message31_header.update({'elevation_angle':radial[i]['radial_header']['Elevation']})
        message31_header.update({'radial_blanking':0})
        message31_header.update({'azimuth_mode':25})     #这项不确定
        message31_size = _structure_size(MSG_31)
        #所有数据长度类的信息暂时没输入,须先讨论有哪些data block存在
        
        #vol
        vol_header = {}
        vol_header.update({'block_type':'R','data_name':'VOL','lrtup':44})
        vol_header.update({'version_major':generic['Major_version']})
        vol_header.update({'version_minor':generic['Minor_version']})
        vol_header.update({'lat':site['Latitude']})
        vol_header.update({'lon':site['Longitude']})
        vol_header.update({'height':site['Grand_h']})
        vol_header.update({'feedhorn_height':site['Antenna_h']})
        vol_header.update({'refl_calib':1.0,'power_h':0.0,'power_v':0.0})      #不确定，原数据找不到,和例子一致
        vol_header.update({'diff_refl_calib':task['ZDR_calibration']})    #不确定
        vol_header.update({'init_phase':task['PHI_calibration']})         #不确定
        vol_header.update({'vcp':vcp,'spare':'aa'})    #不确定，原数据找不到,和例子一致
        
        sum_bit = sum_bit+message31_size
        message31_header.update({'block_vol':sum_bit})
        block_count = block_count+1
        
        #elv
        elv_header = {}
        elv_header.update({'block_type':'R','data_name':'ELV','lrtup':12})
        elv_header.update({'atmos':int(cut[j]['Atmos_loss'])})
        elv_header.update({'refl_calib':-44.625})        #不确定，原数据找不到,和例子一致  
          
        sum_bit = sum_bit+vol_header['lrtup']
        message31_header.update({'block_elv':sum_bit})
        block_count = block_count+1
        
        #RAD
        rad_header = {}
        rad_header.update({'block_type':'R','data_name':'RAD','lrtup':28})
        rad_header.update({'noise_h':task['Horizontal_noise']})
        rad_header.update({'noise_v':task['Vertical_noise']}) 
        rad_header.update({'unambig_range':int(cut[j]['Maximum_range1']/1000)})
        rad_header.update({'nyquist_vel':int(cut[j]['Nyquist_speed'])})
        rad_header.update({'spare':'aa'})
        rad_header.update({'horizontal_channel':-44.95}) #不确定，原数据找不到,和例子一致 
        rad_header.update({'vertical_channel':-44.73})   #不确定，原数据找不到,和例子一致 
        
        sum_bit = sum_bit+elv_header['lrtup']
        message31_header.update({'block_rad':sum_bit})
        sum_bit = sum_bit+rad_header['lrtup']
        block_count = block_count+1
        
        #REF
        var = 'dBZ'
        if var not in radial[i].keys():
            ref_header = {}
            message31_header.update({'block_ref':0})
        else:
            ref_header = {}
            ref_header.update({'block_type':'D','data_name':'REF','reserved':0})
            ref_header.update({'ngates':len(radial[i][var]['data'])})
            ref_header.update({'first_gate':int(cut[j]['Start_range']+cut[j]['Log_resolution'])})
            ref_header.update({'gate_spacing':int(cut[j]['Log_resolution'])})
            ref_header.update({'thresh':100,'snr_thres':16,'flags':0})      #不确定，原数据找不到,和例子一致
            ref_header.update({'word_size':8*radial[i][var]['Bin_length']})
            ref_header.update({'scale':float(radial[i][var]['Scale'])})
            ref_header.update({'offset':float(radial[i][var]['Offset'])})
            ref_header.update({'data':radial[i][var]['data']})       #导入数据
            ref_header.update({'Bin_length':radial[i][var]['Bin_length']})
            
            message31_header.update({'block_ref':sum_bit})
            ref_size = _structure_size(GENERIC_DATA_BLOCK)
            sum_bit = sum_bit + ref_size + radial[i][var]['Length']
            block_count = block_count+1
            
        #vel
        var = 'V'
        if var not in radial[i].keys():
            vel_header = {}
            message31_header.update({'block_vel':0})
        else:
            vel_header = {}
            vel_header.update({'block_type':'D','data_name':'VEL','reserved':0})
            vel_header.update({'ngates':len(radial[i][var]['data'])})
            vel_header.update({'first_gate':int(cut[j]['Start_range']+cut[j]['Doppler_resolution'])})
            vel_header.update({'gate_spacing':int(cut[j]['Doppler_resolution'])})
            vel_header.update({'thresh':100,'snr_thres':16,'flags':0})      #不确定，原数据找不到,和例子一致
            vel_header.update({'word_size':8*radial[i][var]['Bin_length']})
            vel_header.update({'scale':float(radial[i][var]['Scale'])})
            vel_header.update({'offset':float(radial[i][var]['Offset'])})
            vel_header.update({'data':radial[i][var]['data']})       #导入数据
            vel_header.update({'Bin_length':radial[i][var]['Bin_length']})
            
            message31_header.update({'block_vel':sum_bit})
            vel_size = _structure_size(GENERIC_DATA_BLOCK)
            sum_bit = sum_bit + vel_size + radial[i][var]['Length']
            block_count = block_count+1
            
        #SW
        var = 'SW'
        if var not in radial[i].keys():
            sw_header = {}
            message31_header.update({'block_sw':0})
        else:
            sw_header = {}
            sw_header.update({'block_type':'D','data_name':'SW','reserved':0})
            sw_header.update({'ngates':len(radial[i][var]['data'])})
            sw_header.update({'first_gate':int(cut[j]['Start_range']+cut[j]['Doppler_resolution'])})
            sw_header.update({'gate_spacing':int(cut[j]['Doppler_resolution'])})
            sw_header.update({'thresh':100,'snr_thres':16,'flags':0})      #不确定，原数据找不到,和例子一致
            sw_header.update({'word_size':8*radial[i][var]['Bin_length']})
            sw_header.update({'scale':float(radial[i][var]['Scale'])})
            sw_header.update({'offset':float(radial[i][var]['Offset'])})
            sw_header.update({'data':radial[i][var]['data']})       #导入数据
            sw_header.update({'Bin_length':radial[i][var]['Bin_length']})
            
            message31_header.update({'block_sw':sum_bit})
            sw_size = _structure_size(GENERIC_DATA_BLOCK)
            sum_bit = sum_bit + sw_size + radial[i][var]['Length']    
            block_count = block_count+1
            
        #ZDR
        var = 'ZDR'
        if var not in radial[i].keys():
            zdr_header = {}
            message31_header.update({'block_zdr':0})
        else:
            zdr_header = {}
            zdr_header.update({'block_type':'D','data_name':'ZDR','reserved':0})
            zdr_header.update({'ngates':len(radial[i][var]['data'])})
            zdr_header.update({'first_gate':int(cut[j]['Start_range']+cut[j]['Doppler_resolution'])})
            zdr_header.update({'gate_spacing':int(cut[j]['Doppler_resolution'])})
            zdr_header.update({'thresh':100,'snr_thres':16,'flags':0})      #不确定，原数据找不到,和例子一致
            zdr_header.update({'word_size':8*radial[i][var]['Bin_length']})
            zdr_header.update({'scale':float(radial[i][var]['Scale'])})
            zdr_header.update({'offset':float(radial[i][var]['Offset'])})
            zdr_header.update({'data':radial[i][var]['data']})       #导入数据
            zdr_header.update({'Bin_length':radial[i][var]['Bin_length']})
            
            message31_header.update({'block_zdr':sum_bit})
            zdr_size = _structure_size(GENERIC_DATA_BLOCK)
            sum_bit = sum_bit + zdr_size + radial[i][var]['Length']       
            block_count = block_count+1
            
        #phi
        var = 'FDP'
        if var not in radial[i].keys():
            phi_header = {}
            message31_header.update({'block_phi':0})
        else:
            phi_header = {}
            phi_header.update({'block_type':'D','data_name':'PHI','reserved':0})
            phi_header.update({'ngates':len(radial[i][var]['data'])})
            phi_header.update({'first_gate':int(cut[j]['Start_range']+cut[j]['Doppler_resolution'])})
            phi_header.update({'gate_spacing':int(cut[j]['Doppler_resolution'])})
            phi_header.update({'thresh':100,'snr_thres':16,'flags':0})      #不确定，原数据找不到,和例子一致
            phi_header.update({'word_size':8*radial[i][var]['Bin_length']})
            phi_header.update({'scale':float(radial[i][var]['Scale'])})
            phi_header.update({'offset':float(radial[i][var]['Offset'])})
            phi_header.update({'data':radial[i][var]['data']})       #导入数据
            phi_header.update({'Bin_length':radial[i][var]['Bin_length']})
            
            message31_header.update({'block_phi':sum_bit})
            phi_size = _structure_size(GENERIC_DATA_BLOCK)
            sum_bit = sum_bit + phi_size + radial[i][var]['Length']         
            block_count = block_count+1
            
        #rho
        var = 'CC'
        if var not in radial[i].keys():
            rho_header = {}
            message31_header.update({'block_rho':0})
        else:
            rho_header = {}
            rho_header.update({'block_type':'D','data_name':'RHO','reserved':0})
            rho_header.update({'ngates':len(radial[i][var]['data'])})
            rho_header.update({'first_gate':int(cut[j]['Start_range']+cut[j]['Doppler_resolution'])})
            rho_header.update({'gate_spacing':int(cut[j]['Doppler_resolution'])})
            rho_header.update({'thresh':100,'snr_thres':16,'flags':0})      #不确定，原数据找不到,和例子一致
            rho_header.update({'word_size':8*radial[i][var]['Bin_length']})
            rho_header.update({'scale':float(radial[i][var]['Scale'])})
            rho_header.update({'offset':float(radial[i][var]['Offset'])})
            rho_header.update({'data':radial[i][var]['data']})       #导入数据
            rho_header.update({'Bin_length':radial[i][var]['Bin_length']})
            
            message31_header.update({'block_rho':sum_bit})
            rho_size = _structure_size(GENERIC_DATA_BLOCK)
            sum_bit = sum_bit + rho_size + radial[i][var]['Length']    
            block_count = block_count+1
        message31_header.update({'block_count':block_count})
        message31_header.update({'radial_length':sum_bit})
        message_header.update({'size':int((sum_bit+_structure_size(MSG_HEADER))/2)})
        
        temp.update({'message_header':message_header,'message31_header':message31_header})
        temp.update({'vol_header':vol_header,'elv_header':elv_header,'rad_header':rad_header})
        temp.update({'ref_header':ref_header,'vel_header':vel_header,'sw_header':sw_header})
        temp.update({'zdr_header':zdr_header,'phi_header':phi_header,'rho_header':rho_header})
        message.append(temp)
    print('Thread-1:成功转换数据至message31类型\n')
    return location,volumn,message,site_code

def write2arv(volumn_header,message,PATH_OUTPUT,LOCATION,site_code):
    #获取时间字符串
    delta = dt.timedelta(volumn_header['date']-1,volumn_header['time']/1000)
    date = dt.datetime(1970,1,1,0,0,0)+delta
    year = date.strftime('%Y')
    month = date.strftime('%m')
    day = date.strftime('%d') 
    hour = date.strftime('%H')
    minute = date.strftime('%M')
    second = date.strftime('%S')
    output_name = LOCATION+'.'+year+month+day+'.'+hour+minute+second+'.ar2v' #数据导出的名字
    path_write = PATH_OUTPUT+'\\'+LOCATION1[site_code]     #数据导出的路径名
    if not os.path.exists(path_write):
        os.makedirs(path_write)
    path_write = path_write+'\\'+output_name     #数据导出的绝对路径
    
    
    #开始写入
    s = '            ' #间隔
    byte_spare = struct.pack('>12s',bytes(s.encode('utf-8')))
    
    with open(path_write,'wb') as f:
        #写入总的头文件volumn_header
        _pack_structure(f,VOLUME_HEADER,volumn_header)
        #循环写入所有message信息
        for i in np.arange(0,len(message)):
            f.write(byte_spare)
            temp = message[i]
            #输入message的头文件
            _pack_structure(f,MSG_HEADER,temp['message_header'])
            
            #输入message31的头文件
            _pack_structure(f,MSG_31,temp['message31_header'])
            
            #输入vol的头文件
            _pack_structure(f,VOLUME_DATA_BLOCK,temp['vol_header'])
            
            #输入elv的头文件
            _pack_structure(f,ELEVATION_DATA_BLOCK,temp['elv_header'])
          
            #输入rad的头文件
            _pack_structure(f,RADIAL_DATA_BLOCK,temp['rad_header'])
            
            #输入ref的头文件
            if temp['message31_header']['block_ref']!=0:
                _pack_structure(f,GENERIC_DATA_BLOCK,temp['ref_header'])
                data = temp['ref_header']['data']
                if temp['ref_header']['Bin_length']==1: 
                    for j in np.arange(0,len(data)):
                        f.write(struct.pack('> B',data[j]))
                elif temp['ref_header']['Bin_length']==2: 
                    for j in np.arange(0,len(data)):
                        f.write(struct.pack('> H',data[j]))
                            
                
            #输入vel的头文件
            if temp['message31_header']['block_vel']!=0:
                _pack_structure(f,GENERIC_DATA_BLOCK,temp['vel_header'])
                data = temp['vel_header']['data']
                if temp['vel_header']['Bin_length']==1: 
                    for j in np.arange(0,len(data)):
                        f.write(struct.pack('> B',data[j]))
                elif temp['vel_header']['Bin_length']==2: 
                    for j in np.arange(0,len(data)):
                        f.write(struct.pack('> H',data[j]))
          
             #输入sw的头文件
            if temp['message31_header']['block_sw']!=0:
                _pack_structure(f,GENERIC_DATA_BLOCK,temp['sw_header'])
                data = temp['sw_header']['data']
                if temp['sw_header']['Bin_length']==1: 
                    for j in np.arange(0,len(data)):
                        f.write(struct.pack('> B',data[j]))
                elif temp['sw_header']['Bin_length']==2: 
                    for j in np.arange(0,len(data)):
                        f.write(struct.pack('> H',data[j]))
           
            #输入zdr的头文件
            if temp['message31_header']['block_zdr']!=0:
                _pack_structure(f,GENERIC_DATA_BLOCK,temp['zdr_header'])
                data = temp['zdr_header']['data']
                if temp['zdr_header']['Bin_length']==1: 
                    for j in np.arange(0,len(data)):
                        f.write(struct.pack('> B',data[j]))
                elif temp['zdr_header']['Bin_length']==2: 
                    for j in np.arange(0,len(data)):
                        f.write(struct.pack('> H',data[j]))
         
            #输入phi的头文件
            if temp['message31_header']['block_phi']!=0:
                _pack_structure(f,GENERIC_DATA_BLOCK,temp['phi_header'])
                data = temp['phi_header']['data']
                if temp['phi_header']['Bin_length']==1: 
                    for j in np.arange(0,len(data)):
                        f.write(struct.pack('> B',data[j]))
                elif temp['phi_header']['Bin_length']==2: 
                    for j in np.arange(0,len(data)):
                        f.write(struct.pack('> H',data[j]))
         
            #输入rho的头文件
            if temp['message31_header']['block_rho']!=0:
                _pack_structure(f,GENERIC_DATA_BLOCK,temp['rho_header'])      
                data = temp['rho_header']['data']
                if temp['rho_header']['Bin_length']==1: 
                    for j in np.arange(0,len(data)):
                        f.write(struct.pack('> B',data[j]))
                elif temp['rho_header']['Bin_length']==2: 
                    for j in np.arange(0,len(data)):
                        f.write(struct.pack('> H',data[j]))
    print('Thread-1:成功写入数据至.ar2v格式\n',path_write)
    print('Thread-1:----------------------------------------------------------------------\n')