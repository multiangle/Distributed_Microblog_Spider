__author__ = 'multiangle'


#========================================================
#
#    This file contains the option info for client.
#
#========================================================

# 全局参数              global config info
PROCESS_NUM             =3                #进程数目           number of process
THREAD_NUM              =10                #每个进程最多线程   max thread num per process
NOMAL_INFO_PRINT        =True            #普通信息显示       if print normal information
KEY_INFO_PRINT          =True            #关键信息显示       if print key information
NORMAL_INFO_LOG         =True            #普通信息日志       if output normal info to log
KEY_INFO_LOG            =True            #错误信息日志       if output key info to log
LOG_POS                 ='log\\'          #日志存放点         the address of log
DATA_POS                ='temp\\'       #临时数据存放点

#代理相关               about proxy
USE_PROXY               =True            #是否使用代理        if use proxy
PROXY_POOL_SIZE         =THREAD_NUM*2     #每个进程维持的代理池的大小


####-------------------------------------####

# SERVER_URL='http://multiangle.imwork.net:10407' #服务器地址,端口号
# SERVER_URL='http://121.42.163.195:8000'
SERVER_URL='http://127.0.0.1:8000' #服务器地址,端口号



