__author__ = 'multiangle'

"""
    NAME:       server_database
    PY_VERSION: python3.4
    FUNCTION:--------------------------------------------------------------
    This file deal with the event of databases.
    The several main task is:
    1. Check cahce_attends, and check if the user exists in table of ready_to_get
    and user_info_table. If not , store it into ready_to_get table
    2. Check the data stored in cache_user_info, and store then into user_info_table.
    Be sure that the user in user_info_table should be unique. And delet the user in
    ready_to_get table
    3. Check the data stored in cache_user_get , and store then into atten_web table.
    Also, the attend connection of two people should be unique
    4. Check the ready_to_get table . if some uid is fetching for too much time, set
    the value if is_fetching to null
    -----------------------------------------------------------------------
    VERSION:    _0.2_
    UPDATE_HISTORY:
        _0.2:   add redis and bloom filter as the cache of mysql
        _0.1_:  The 1st edition
"""
#======================================================================
#----------------import package--------------------------
# import python package
import urllib.request as request
import urllib.parse as parse
from multiprocessing import Process
import threading
import time
import redis
import os
import json
import http.cookiejar
import re
import random
import sys
from pymongo import MongoClient
from pymongo import UpdateOne,UpdateMany

# import from this folder
import client_config as config
import File_Interface as FI
from DB_Interface import MySQL_Interface
#=======================================================================

#=======================================================================
#---code session
class deal_cache_attends(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        dbi=MySQL_Interface()
        self.dbi=dbi
        self.bf=BloomFilter()

    def  run(self):
        bag=[]
        uid_bag=[]              #与bag类似，只不过存储uid
        bag_size=1000             #100次插入一次
        ready_to_get_col=self.dbi.get_col_name('ready_to_get')
        cache_attends_col=self.dbi.get_col_name('cache_attends')
        while True:
            query='select * from cache_attends limit 5000'
            res=self.dbi.select_asQuery(query)
            if res.__len__()==0:
                if bag.__len__()>0:
                    self.dbi.insert_asList('ready_to_get',bag,unique=True)
                    bag=[]
                    # self.bf.insert_asList(uid_bag,'ready_to_get')
                    uid_bag=[]
                time.sleep(1)
                self.dbi=MySQL_Interface()  #更新dbi
                continue

            print('thread cache attends is working')

            for line in res:
                raw_id=line[cache_attends_col.index('uid')]
                in_user_info=self.bf.isContains(raw_id,'user_info_table')   #此处可优化
                if not in_user_info:
                    data=[line[cache_attends_col.index(col)] if col in cache_attends_col else None for col in ready_to_get_col]
                    bag.append(data)
                    uid_bag.append(raw_id)
                    if bag.__len__()>bag_size:
                        self.dbi.insert_asList('ready_to_get',bag,unique=True)
                        # self.bf.insert_asList(uid_bag,'ready_to_get')
                        print('insert once')
                        bag=[]
                        uid_bag=[]
                self.dbi.delete_line('cache_attends','uid',raw_id) # 此处可优化

    def isInUserInfo(self,in_uid):
        col_user_info=self.dbi.get_col_name('user_info_table')
        query='select * from user_info_table where uid={uid}'.format(uid=in_uid)
        res=self.dbi.select_asQuery(query)
        if res.__len__()==0:
            return False
        else:
            return True

class deal_cache_user_info(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.dbi=MySQL_Interface()
        self.bf=BloomFilter()

    def run(self):
        while True:
            if self.dbi.is_empty('cache_user_info'):
                time.sleep(2)
                self.dbi=MySQL_Interface()
                continue
            [res,cache_user_info_col]=self.dbi.select_all('cache_user_info')

            time_stick=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))      # insert into user info table
            user_info_table_col=self.dbi.get_col_name('user_info_table')
            data= [
                    [
                        line[cache_user_info_col.index(col)] if col in cache_user_info_col
                        else time_stick if col=='insert_time'
                        else None if col=='update_time'
                        else None if col=='latest_blog'
                        else None if col=='isGettingBlog'
                        else ''
                        for col in user_info_table_col
                    ] for line in res]
            uid_list=[line[user_info_table_col.index('uid')] for line in data]
            self.dbi.insert_asList('user_info_table',data,unique=True)          # 插入 user info table
            self.bf.insert_asList(uid_list,'user_info_table')
            print('insert {num} users into user info table'.format(num=data.__len__()))

            uid_list=[line[cache_user_info_col.index('uid')] for line in res]
            q1="delete from {table_name} where uid in ( {id_str_list} ) ;"   # 从cache user info 中删除
            id_str_list=''
            for i in uid_list:
                id_str_list=id_str_list+'\''+str(i)+'\''+','
            id_str_list=id_str_list[:-1]

            query=q1.format(id_str_list=id_str_list,table_name='cache_user_info')
            self.dbi.cur.execute(query)
            self.dbi.conn.commit()

            query=q1.format(id_str_list=id_str_list,table_name='ready_to_get')
            self.dbi.cur.execute(query)
            self.dbi.conn.commit()

class deal_fetching_user(threading.Thread):
    #定期清理获取时间过长的部分

    def __init__(self):
        threading.Thread.__init__(self)
        self.dbi=MySQL_Interface()

    def run(self):
        while True:
            self.dbi=MySQL_Interface()
            t=time.time()
            time_stick=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t-3600))
            query="update ready_to_get set is_fetching=null where is_fetching < \'{time}\' ;".format(time=time_stick)
            # print(query)
            # query='select * from ready_to_get where is_fetching < {time}'.format(time=time_stick)
            self.dbi.update_asQuery(query)
            time.sleep(1)

class control_ready_table(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.dbi=MySQL_Interface()
    def run(self):
        while True:
            self.dbi=MySQL_Interface()
            num=self.dbi.get_line_num('ready_to_get')
            if num>150*1000:
                query='select m.fans_num from (' \
                      'select fans_num from ready_to_get ' \
                      'ORDER BY fans_num limit 50000' \
                      ') as m order by fans_num desc limit 1'
                res=self.dbi.select_asQuery(query)[0][0]
                query='delete from ready_to_get where fans_num<{num}'\
                    .format(num=res)
                self.dbi.update_asQuery(query)
            else:
                time.sleep(600)

class deal_isGettingBLog_user(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.dbi=MySQL_Interface()

    def run(self):
        while True:
            self.dbi=MySQL_Interface()
            t=time.time()
            time_stick=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(t-12*60*60))

            #删掉cache_history中的行
            query='delete from cache_history where container_id in (select container_id from user_info_table where isGettingBlog<\'{time}\' and update_time is null)'\
                .format(time=time_stick)
            self.dbi.update_asQuery(query)

            # 删掉mongodb-assemble factory中的相关值
            select_query='select container_id from user_info_table where isGettingBlog<\'{time}\' and update_time is null'.format(time=time_stick)
            res=[x[0] for x in self.dbi.select_asQuery(select_query)]
            client=MongoClient('localhost',27017)
            db=client['microblog_spider']
            assemble_table=db.assemble_factory
            assemble_table.remove({'container_id':{'$in':res}})

            # 将user info table中超时行的isGettingBlog清空
            query="update user_info_table set isGettingBlog=null where isGettingBlog<\'{time}\' and update_time is null".format(time=time_stick)
            self.dbi.update_asQuery(query)

            # 将cache_history中的残留项去除
            query = "delete from cache_history where is_dealing<\'{time}\' ;".format(time = time_stick)
            self.dbi.update_asQuery(query)

            time.sleep(60)

class deal_cache_history(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while True:
            dbi=MySQL_Interface()
            col_info=dbi.get_col_name('cache_history')
            query='select * from cache_history where is_dealing is null order by checkin_timestamp limit 1'

            mysql_res=dbi.select_asQuery(query)
            if mysql_res.__len__()==0:       # cache_history表为空时，睡眠1秒,跳过此次循环
                time.sleep(1)
                continue

            mysql_res=mysql_res[0]

            # todo for delete-----
            print('debug->start to deal with a new task')
            print('debug->mysql_res: ')
            print(mysql_res)
            #------------------------

            container_id=mysql_res[col_info.index('container_id')]
            print('debug->container_id: {cid}'.format(cid=container_id))
            latest_time=mysql_res[col_info.index('latest_time')]
            latest_timestamp=mysql_res[col_info.index('latest_timestamp')]
            time_stick=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
            query = 'update cache_history set is_dealing=\'{time}\' where container_id={cid}'.format(time=time_stick, cid = container_id)
            # todo for delete-----
            print('debug->query1 : {q}'.format(q=query))
            # ------------------------
            dbi.update_asQuery(query)

            client = MongoClient('localhost', 27017)
            db = client['microblog_spider']
            assemble_table = db.assemble_factory
            res = assemble_table.find({'container_id': container_id}, {'current_id': 1, 'total_num': 1})
            id_list = [x['current_id'] for x in res]
            num = int([x['total_num'] for x in assemble_table.find({'container_id': container_id}).limit(1)][0])
            ## todo for delete-----
            print('debug->id_list_len: {len}'.format(len=id_list.__len__()))
            print('debug->num: {n}'.format(n=num))
            # ------------------------
            # 检查是否所有包裹已经到齐
            check_state = True
            if id_list.__len__() < num:
                print('server->HistoryReport:The package is not complete, retry to catch data')
                check_state = False

            if check_state:
                # 如果所有子包已经收集完毕，则将数据放入正式数据库mongodb
                # 将装配车间中的相关数据删除
                # 并且在Mysql中更新update_time和latest_blog,抹掉isGettingBlog

                # 从mysql获取该用户信息
                try:
                    query = 'select * from user_info_table where container_id=\'{cid}\'' \
                        .format(cid=container_id)
                    user_info = dbi.select_asQuery(query)[0]
                    # todo fro debug-------------
                    print('debug->query2: {q}'.format(q=query))
                    print('debug->user_info:')
                    print(user_info)
                    # --------------------------------
                    col_name = dbi.get_col_name('user_info_table')
                except Exception as e:
                    print('Error:server-HistoryReturn:'
                          'No such user in MySQL.user_info_table,Reason:')
                    print(e)

                # 将数据从assemble factory中提取出来
                try:
                    data_list = assemble_table.find({'container_id':container_id}, {'data': 1 , 'current_id': 1})
                    data_list_ori = [x for x in data_list]
                    data_list = [x['data'] for x in data_list_ori]
                    id_list = [x['current_id'] for x in data_list_ori]
                    data_list_ori = None
                    # todo fro debug-------------
                    print('debug->datalist: {len}'.format(len = data_list.__len__()))
                    # --------------------------------
                except Exception as e:
                    print('Error:server-HistoryReturn:'
                        'Unable to get data from MongoDB, assemble factory,Reason:')
                    print(e)

                #　长度大于预期，说明有重复信息，需要去重
                if  id_list.__len__() > num :
                    unique_data_list = []
                    check_dict = {}
                    for i in range(id_list.__len__()) :
                        try:
                            # 这里使用字典去重，（算是hash吧）
                            check_dict[str(id_list[i])]
                            continue
                        except:
                            check_dict[str(id_list[i])] = True
                            unique_data_list.append(data_list[i])
                            # print('data_list.len :{len}'.format(len=data_list.__len__()))
                            # print('id_list.len :{len}'.format(len=id_list.__len__()))
                            # print(i)
                    data_list = unique_data_list

                # 将碎片拼接
                try:
                    data_final = []
                    for i in data_list:
                        data_final = data_final+i
                    # todo fro debug-------------
                    print('debug->数据拼接完毕,len {len}'.format(len=data_final.__len__()))
                    # --------------------------------
                except Exception as e:
                    print('Error:server-HistoryReport:'
                          'Unable to contact the pieces of information，Reason:')
                    print(e)

                # 将本次信息录入accuracy_table 用以进一步分析
                blog_len = data_final.__len__()
                wanted_blog_len = user_info[col_name.index('blog_num')]
                blog_accuracy = blog_len/wanted_blog_len
                time_stick = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                query = 'insert into accuracy_table values ({acc},\'{t_s}\',{num}) ;' \
                    .format(acc=blog_accuracy, t_s=time_stick, num=wanted_blog_len)
                dbi.insert_asQuery(query)

                # 将数据录入Mongodb 更改Mysql,删除assemble中相关内容
                try:
                    if not user_info[col_name.index('update_time')]:
                        # 将数据存入 Mongodb 的formal collection
                        save_data_seperately(data_final)
                        print('Success: Data has saved in Mongodb, size is {size}'
                              .format(size=sys.getsizeof(data_final)))

                        # 将数据从assemble factory 去掉
                        assemble_table.remove({'container_id':container_id})
                        print('Success: Data has been removed from assemble factory')

                        # # 将关键信息录入Mydql
                        query = 'update user_info_table set ' \
                              'update_time=\'{up_time}\',' \
                              'latest_blog=\'{latest_blog}\',' \
                              'isGettingBlog=null ' \
                              'where container_id=\'{cid}\';'\
                            .format(up_time=time_stick,latest_blog=latest_time,cid=container_id)
                        # query='update user_info_table set ' \
                        #       'update_time=\'{up_time}\',' \
                        #       'latest_blog=\'{latest_blog}\'' \
                        #       'where container_id=\'{cid}\';' \
                        #     .format(up_time=time_stick,latest_blog=latest_time,cid=container_id)
                        #TODO 这里为了方便统计，去掉了抹除isGetting这一项，但是正式运行的时候是要加上的
                        dbi.update_asQuery(query)
                        print('Success: insert user into MongoDB, the num of data is {len}'
                              .format(len=blog_len))
                    else:
                        query='update user_info_table set isGettingBlog=null where container_id=\'{cid}\'' \
                            .format(cid=container_id)
                        dbi.update_asQuery(query)

                except Exception as e:
                    print('Error:server->HistoryReport:'
                          'Reason:')
                    print(e)
            else:
                # 如果所有子包不全，则抹掉isGettingBlog,将装配车间中数据删除,去掉cache_history中相应行
                query='update user_info_table set isGettingBlog=null where container_id=\'{cid}\'' \
                    .format(cid=container_id)
                dbi.update_asQuery(query)
                assemble_table.remove({'container_id':container_id})

            # 将cache_history中的相应行删掉，表示已经处理完该事物了
            query='delete from cache_history where container_id=\'{cid}\'' \
                .format(cid=container_id)
            dbi.update_asQuery(query)

class deal_update_mission(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        client=MongoClient('localhost',27017)
        while True:
            db=client['microblog_spider']
            mission_mongo=db.update_mission
            # 表示需要处理，但是现在无人处理的任务
            res=mission_mongo.find({'isReported':{'$ne':None},'isDealing':None}).limit(1)
            res=[x for x in res]

            # 若没有待完成的任务，则该线程休眠1秒然后继续
            if res.__len__()==0:
                time.sleep(1)
                continue

            # 提取出需要处理的任务
            task=res[0]
            task.pop('_id')
            mission_id=task['mission_id']
            user_content=task['user_list']

            # 将任务列表中的isDealing设置当前时间，表示当前任务开始受理
            mission_mongo.update({'mission_id':mission_id},{'$set':{'isDealing':int(time.time())}})

            # 获取包裹id和总包裹数
            assemble_table=db.assemble_factory
            res=assemble_table.find({'container_id':mission_id},{'current_id':1,'total_num':1})
            id_list=[x['current_id'] for x in res]
            num=int([x['total_num'] for x in assemble_table.find({'container_id':mission_id}).limit(1)][0])

            #检查是否所有包裹已经到齐
            check_state=True
            if id_list.__len__()<num:
                print('server->HistoryReport:The package is not complete, retry to catch data')
                check_state=False

            if check_state:
                # 增加当前时间的转发，点赞和评论数，便于追踪
                # 如果所有子包完毕，则将数据放入正式数据库mongodb各已经收集月份表和最近半月表

                # 将数据从assemble factory中提取出来
                try:
                    data_list = assemble_table.find({'container_id':mission_id}, {'data': 1 , 'current_id': 1})
                    data_list_ori = [x for x in data_list]
                    data_list = [x['data'] for x in data_list_ori]
                    id_list = [x['current_id'] for x in data_list_ori]
                    data_list_ori = None
                    print('success->datalist: {len}'.format(len=data_list.__len__()))
                except Exception as e:
                    print('Error:server_database-deal_update_mission:'
                          'Unable to get data from MongoDB, assemble factory,Reason:')
                    print(e)

                #　长度大于预期，说明有重复信息，需要去重
                if id_list.__len__() > num :
                    unique_data_list = []
                    check_dict = {}
                    for i in range(id_list.__len__()) :
                        try:
                            # 这里使用字典去重，（算是hash吧）
                            check_dict[str(id_list[i])]
                            continue
                        except:
                            check_dict[str(id_list[i])] = True
                            unique_data_list.append(data_list[i])
                    data_list = unique_data_list

                # 将碎片拼接
                try:
                    data_final=[]
                    for i in data_list:
                        data_final=data_final+i
                    print('success->数据拼接完毕,len {len}'.format(len=data_final.__len__()))
                except Exception as e:
                    print('Error:server-HistoryReport:'
                          'Unable to contact the pieces of information，Reason:')
                    print(e)

                # 增加当前时间的转发，点赞和评论数，便于追踪,并制作成UpdateMany对象
                user_list=[x['container_id'] for x in user_content]
                user_list_str=''
                for item in user_list:
                    user_list_str+='\''+str(item)+'\','
                user_list_str=user_list_str[:-1]

                def temp_add_trace(line):
                    msg_id=line['id']
                    current_status=dict(
                        comments_count=line['comments_count'],
                        attitudes_count=line['attitudes_count'],
                        reposts_count=line['reposts_count']
                    )
                    t=int(time.time())
                    t_str=str(t)
                    line['status_trace.{date}'.format(date=t_str)]=current_status
                    update_item=UpdateMany({'id':msg_id},{'$set':line},upsert=True)
                    return update_item

                requests=[temp_add_trace(x) for x in data_final]
                latest_mongo=db.latest_history
                latest_mongo.bulk_write(requests)
                print('Success: server_database:UpdateMany列表生成，写入latest_history表成功,{len}'.format(len=requests.__len__()))

                # 将获得数据写入各按月份分类的聚合中
                table_list=[]
                request_updateMonth=[]
                for i in range(data_final.__len__()):
                    temp_time=data_final[i]['created_at']
                    temp_table_name='user_{year}_{month}'.format(year=temp_time[0:4],month=temp_time[5:7])
                    if temp_table_name in table_list:
                        request_updateMonth[table_list.index(temp_table_name)].append(requests[i])
                    else:
                        table_list.append(temp_table_name)
                        request_updateMonth.append([requests[i]])
                print('the number of table is {len}'.format(len=request_updateMonth.__len__()))

                for i in range(table_list.__len__()):
                    collection=eval('db.{name}'.format(name=table_list[i]))
                    # todo for debug----------------------------
                    print('table {x} is started'.format(x=table_list[i]))
                    #---------------------------------------------------
                    if request_updateMonth[i].__len__()>0:
                        collection.bulk_write(request_updateMonth[i])

                print('Success:server_database:所获的数组已经写入按月分类聚合中')

                # 清理Mydql，更新相关行数中的update_time和latest_blog
                time_stick=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(time.time()))
                # 找出各用户的最近更新时间
                latest_list=[0]*user_list.__len__()
                for line in data_final:
                    this_timestick=int(line['created_timestamp'])
                    this_container='100505'+str(line['user_id'])
                    try:
                        index=user_list.index(this_container)
                        if latest_list[index]<this_timestick:
                            latest_list[index]=this_timestick
                    except:
                        print('error:server_database->deal_update_mission:'
                              'container {id} is not in user_list'.format(id=this_container))

                # 将各用户最近更新时间固化为mysql更新语句。
                case_list=''
                updated_user_list=''
                for i in range(latest_list.__len__()):
                    if latest_list[i]>user_content[i]['latest_blog'] :
                        time_stick_inner=time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(latest_list[i]))
                        case_list+=' when \'{cid}\' then \'{tstick}\' '.format(cid=user_list[i],tstick=time_stick_inner)
                        updated_user_list+='\'{cid}\','.format(cid=user_list[i])
                updated_user_list=updated_user_list[:-1]
                # 构建mysql更新语句
                query1='update user_info_table set update_time=\'{time}\' where container_id in ( {user_list} ) ;'\
                    .format(time=time_stick,user_list=user_list_str)
                query2='update user_info_table set latest_blog= case container_id {case_list} end where container_id in ( {ulist2} ) ;'\
                    .format(case_list=case_list,ulist2=updated_user_list)
                dbi=MySQL_Interface()
                dbi.update_asQuery(query2)
                dbi.update_asQuery(query1)
                print('Success:server_database: UpdateTime和LatestBlog选项已更新')
                query='update user_info_table set isGettingBlog=null where container_id in ({user_list});' \
                    .format(user_list=user_list_str)
                dbi.update_asQuery(query)
                print('Success:erver_database: isGettingBlog选项已清除')

            else:
                query='update user_info_table set isGettingBlog=null where container_id in ({user_list});'\
                    .format(user_list=user_list_str)
                dbi=MySQL_Interface()
                dbi.update_asQuery(query)

            # 将assemble_factory中与当前任务有关数据清空
            assemble_table.remove({'container_id':mission_id})
            print('Success:server_database: assemble_factory in Mongo is cleared')

            # 将mongodb，任务列表中当前任务项清空
            mission_mongo.remove({'mission_id':mission_id})
            print('Success:server_database: this mission is cleared')

class clear_expired_update_mission(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while True:
            client=MongoClient('localhost',27017)
            db=client['microblog_spider']
            mission_mongo=db.update_mission
            assemble_mongo=db.assemble_factory
            current_time=int(time.time())
            target_time=current_time-60*60*6   #将6个小时仍未完成的任务清除出去
            expired_mission=mission_mongo.find({'mission_start':{'$lt':target_time}}).limit(1)
            expired_mission=[x for x in expired_mission]
            if expired_mission.__len__()==0:
                # 如果没有符合要求的过期任务，则休眠
                time.sleep(60)
            else:
                # 如果有过期的任务
                expired_mission=expired_mission[0]
                mission_id=expired_mission['mission_id']
                user_content=expired_mission['user_list']
                user_list=[x['container_id'] for x in user_content]

                #　将mysql中相关用户isGettingBlog清空
                user_list_str=''
                for item in user_list:
                    user_list_str+='\''+str(item)+'\','
                user_list_str=user_list_str[:-1]
                dbi=MySQL_Interface()
                query='update user_info_table set isGettingBlog=null where container_id in ({user_list});' \
                    .format(user_list=user_list_str)
                dbi.update_asQuery(query)

                # 将assemble_factory中数据清空
                assemble_mongo.remove({'container_id':mission_id})

                # 将Mongo中该任务从任务表中清空。
                mission_mongo.remove({'mission_id':mission_id})

class clear_expired_update_content(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

    def run(self):
        while True:
            client=MongoClient('localhost',27017)
            db=client['microblog_spider']
            latest_mongo=db.latest_history
            t=int(time.time())-60*60*24*15
            latest_mongo.remove({'created_timestamp':{'$lt':t}})
            time.sleep(600)

class DB_manager(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)

        # p1,p2,p3,p4 used to get the atten web of user in microblog
        self.p1=deal_cache_attends()
        self.p2=deal_cache_user_info()
        self.p3=deal_fetching_user()
        self.p4=control_ready_table()

        # p5, p6 are used to get the historical content of microblog
        self.p5=deal_isGettingBLog_user()
        self.p6=deal_cache_history()

        # p7,p8 used to get the update content of microblog
        self.p7=deal_update_mission()
        self.p8=clear_expired_update_mission()

        self.p9=clear_expired_update_content()

    def run(self):
        self.p1.start()
        self.p2.start()
        self.p3.start()
        self.p4.start()
        self.p5.start()
        self.p6.start()
        self.p7.start()
        self.p8.start()
        self.p9.start()
        print('Process: deal_cache_attends is started ')
        print('Process: deal_cache_user_info is started ')
        print('Process: deal_fetching_user is started')
        print('Process: control_ready_table is started')
        print('Process: deal_isGettingBLog_user is started')
        print('Process: deal_cache_history is started')
        print('Process: deal_update_mission is started')
        print('Process: clear_expired_update_mission')
        print('Process: clear_expired_update_content')

        while True:
            time.sleep(5)
            if not self.p1.is_alive():
                self.p1=deal_cache_attends()
                self.p1.start()
                print('Process: deal_cache_attends is restarted ')
            if not self.p2.is_alive():
                self.p2=deal_cache_user_info()
                self.p2.start()
                print('Process: deal_cache_user_info is restarted ')
            if not self.p3.is_alive():
                self.p3=deal_fetching_user()
                self.p3.start()
                print('Process: deal_fetching_user is restarted')
            if not self.p4.is_alive():
                self.p4=control_ready_table()
                self.p4.start()
                print('Process: control_ready_table is restarted')
            if not self.p5.is_alive():
                self.p5=deal_isGettingBLog_user()
                self.p5.start()
                print('Process: deal_isGettingBlog_user is restarted')
            if not self.p6.is_alive():
                self.p6=deal_cache_history()
                self.p6.start()
                print('Process: deal_cache_history is restarted')
            if not self.p7.is_alive():
                self.p7=deal_update_mission()
                self.p7.start()
                print('Process: deal_update_mission is restarted')
            if not self.p8.is_alive():
                self.p8=clear_expired_update_mission()
                self.p8.start()
                print('Process: clear_expired_update_mission is restarted')
            if not self.p9.is_alive():
                self.p9.start()
                print('Process: clear_expired_update_content')

class SimpleHash():
    def __init__(self,cap,seed):
        self.cap=cap
        self.seed=seed
    def hash(self,value):
        ret=0
        for i in range(value.__len__()):
            ret+=self.seed*ret+ord(value[i])
        return ((self.cap-1) & ret)

class BloomFilter():
    def __init__(self):
        self.bit_size=1<<15
        self.seeds=[5,7,11,13,31,37,61]
        self.r=redis.StrictRedis(host='127.0.0.1',port=6379,db=0)
        self.hashFunc=[]
        for i in range(self.seeds.__len__()):
            self.hashFunc.append(SimpleHash(self.bit_size,self.seeds[i]))

    def isContains(self,str_input,name):
        if str_input==None:
            return False
        if str_input.__len__()==0:
            return False
        ret=True
        for f in self.hashFunc:
            loc=f.hash(str_input)
            ret=ret & self.r.getbit(name,loc)
        return ret

    def insert(self,str_input,name):
        for f in self.hashFunc:
            loc=f.hash(str_input)
            self.r.setbit(name,loc,1)

    def insert_asList(self,list_input,name):
        for line in list_input:
            self.insert(line,name)

def save_data_inMongo(dict_data):
    client=MongoClient('localhost',27017)
    db=client['microblog_spider']
    collection=db.formal
    result=collection.insert_many(dict_data)

def save_data_seperately(dict_data):
    client=MongoClient('localhost',27017)
    db=client['microblog_spider']
    table_list=[]
    data_list=[]
    for line in dict_data:
        temp_time=line['created_at']
        temp_table_name='user_{year}_{month}'.format(year=temp_time[0:4],month=temp_time[5:7])
        if temp_table_name not in table_list:
            table_list.append(temp_table_name)
            sub_data_list=[line]
            data_list.append(sub_data_list)
        else:
            data_list[table_list.index(temp_table_name)].append(line)
    for i in range(table_list.__len__()):
        collection=eval('db.{name}'.format(name=table_list[i]))
        collection.insert_many(data_list[i])

if __name__=='__main__':
    db_thread=DB_manager()              # database thread
    db_thread.start()