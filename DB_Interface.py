__author__ = 'multiangle'
# import mysql.connector
import pymysql
import traceback

class MySQL_Interface:
    def __init__(self,host='127.0.0.1',user='root',pwd='admin',dbname='microblog_spider'):
        self.host=host
        self.user=user
        self.passwd=pwd
        self.db=dbname
        try:
            self.conn=pymysql.connect(
                user=self.user,
                password=self.passwd,
                host=self.host,
                db=self.db,
                charset='utf8mb4'
                )
            self.cur=self.conn.cursor()

        except Exception as e:
            print("ERROR:faile to connect mysql")
            print(e)
            print(traceback.print_exc())

    def __del__(self):
        try:
            self.cur.close()
            self.conn.close()
        except Exception as e:
            print('ERROR:__del__',e)

    def create_table(self,table_name,col_name_list,col_type_list=[]):
        """
        #可以有预设值 0 表示 INT； 1表示 float; 2表示 varchar(255)
        :param table_name:
        :param col_name_list:
        :param col_type_list:
        :return:   1 create successfully
                    0 create fail
        """
        if col_type_list==[]:   #col_type_list默认为空。如果为空，则默认值为varchar(255)
            col_type_list=['varchar(255)']*col_name_list.__len__()
        if col_name_list.__len__()!=col_type_list.__len__():
            print('ERROR:列名与列属性长度不一致！')
            return -1
        q1="create table %s ("%(table_name)
        q2=""
        for i in range(0,col_name_list.__len__()):
            q2=q2+col_name_list[i]+' '
            if col_type_list[i]==0: #可以有预设值 0 表示 int； 1表示 float; 2表示 varchar(100)
                q2=q2+'INT,'
            elif col_type_list[i]==1:
                q2=q2+'FLOAT,'
            elif col_type_list[i]==2:
                q2=q2+'VARCHAR(255),'
            else:
                q2=q2+col_type_list[i]+','
        q2=q2[0:q2.__len__()-1]
        query=q1+q2+');'
        # print(query)
        try:
            self.cur.execute(query)
            self.conn.commit()
            return 1
        except:
            print("ERROR:create_table: 创建表失败")
            return -1

    def drop_table(self,table_name):
        query="drop table %s ;"%(table_name)
        try:
            self.cur.execute(query)
            self.conn.commit()
        except:
            print('ERROR: drop table')

    def get_col_name(self,table_name):
        query="SHOW COLUMNS FROM %s ;"%(table_name)
        try:
            self.cur.execute(query)
        except:
            print('fail to get column info ')
        col_name=[x[0] for x in self.cur.fetchall()]
        return col_name

    def get_line_num(self,table_name):
        query='select count(*) as value from {table_name} ;'.format(table_name=table_name)
        num=-1
        try:
            self.cur.execute(query)
            res=self.cur.fetchall()
            num=res[0][0]
        except Exception as e:
            print('fail to get line num ',e)
        return num


    def select_all(self,table_name,code=''):
        query="select * from %s ;"%(table_name)
        try:
            self.cur.execute(query)
        except Exception as e:
            print('fail to get data from %s'%(table_name))
            print(e)
        data=[x for x in self.cur.fetchall()]
        col_info=self.get_col_name(table_name)
        if code=='':
            return [data,col_info]
        else:
            for i in range(data.__len__()):
                data[i]=self.list_code_transform(data[i],code)
            return [data,col_info]

    def select_asQuery(self,query,code=''):
        try:
            self.cur.execute(query)
            res=[list(x) for x in self.cur.fetchall()]
        except Exception as e:
            print('fail to execute the query')
            print(e)
        if code=='':
            return res
        else:
            for i in range(res.__len__()):
                res[i]=self.list_code_transform(res[i],code)
            return res

    def add_col(self,table_name,new_col_name,new_col_property):
        col_info=self.get_col_name(table_name)
        if new_col_name in col_info:
            print('WARNING:import_data.MSSQL_Interface.add_col:  待插入列已经存在')
        else:
            query="alter table %s add column %s %s"%(table_name,new_col_name,new_col_property)
            try:
                self.cur.execute(query)
                self.conn.commit()
            except Exception as e:
                print('fail to add col')
                print(e)

    def drop_col(self,table_name,col_name):
        col_info=self.get_col_name(table_name)
        if col_name not in col_info:
            print('WARNING:待删除列不存在！')
        else:
            query="alter table %s drop column %s;"%(table_name,col_name)
            try:
                self.cur.execute(query)
                self.conn.commit()
            except Exception as e:
                print('faile to drop column')
                print(e)

    def alter_col_property(self,table_name,target_col,target_property):
        col_info=self.get_col_name(table_name)
        if target_col in col_info:
            query="alter table %s modify %s %s"%(table_name,target_col,target_property)
            try:
                self.cur.execute(query)
                self.conn.commit()
            except Exception as e:
                print('fail to alter column property ', e)
        else:
            print('target col not exist in table!')

    def update_content(self,table_name,target_col_name,target_col_value,pos_col_name,pos_col_value):
        query="update %s set %s=%s where %s=%s ;"%(table_name,target_col_name,target_col_value,pos_col_name,pos_col_value)
        try:
            self.cur.execute()
            self.conn.commit()
        except Exception as e:
            print('fail to update content ', e)

    def update_asQuery(self,query):
        try:
            self.cur.execute(query)
            self.conn.commit()
        except Exception as e:
            print(query)
            print('Unable to update conent',e)

    # def update_content_asList(self,table_name,param_list):
    #     query="update %s set %s=%s where %s=%s ;"
    #     try:
    #         self.cur.executemany(query,param_list)
    #         self.conn.commit()
    #     except Exception as e:
    #         print(e)

    def insert_asList(self,table_name,data_list,unique=False):
        if data_list.__len__()==0: #check the length of data list
            print('the length of data is 0')
            return -1
        if data_list[0]==0:      # the the length of columns in data list
            print('the length of columns is 0')
            return -1
        len_set=set([x.__len__() for x in data_list])
        if len_set.__len__()>1:    #check if the data list is aligned
            print('array is not aligned')
            return -1
        if isinstance(data_list[0],list):
            #check if the data type in [[]..[]] transform to [()..()]
            data_list=[tuple(x) for x in data_list]
        if unique:
            q1="insert ignore into %s values ("%(table_name)
        else:
            q1="insert into %s values ("%(table_name)
        q2="%s,"*(data_list[0].__len__()-1)+"%s)"
        query=q1+q2
        try:
            self.cur.executemany(query,data_list)
            self.conn.commit()
        except Exception as e:
            print("fail to insert data", e)

    def insert_asQuery(self,query):
        try:
            self.cur.execute(query)
            self.conn.commit()
        except Exception as e:
            print('fail to insert as query,',e)

    def delete_line(self,table_name,col_name,col_value):
        query="delete from {table_name} where {col}=\'{col_value}\'"\
            .format(table_name=table_name,col=col_name,col_value=col_value)
        self.cur.execute(query)
        self.conn.commit()

    def list_code_transform(self,strlist,codec='gb2312'):
        # len=strlist.__len__()
        out=[]
        for item in strlist:
            p=self.code_transform(item,codec)
            out.append(p)
        return out

    def code_transform(self,strText,codec='gb2312'):
        b = bytes((ord(i) for i in strText))
        return b.decode(codec)

    def is_empty(self,table_name):
        query='select * from {tname} limit 1 ;'.format(tname=table_name)
        res=self.select_asQuery(query)
        if res.__len__()==0:
            return True
        else:
            return False

if __name__=='__main__':
    mi=MySQL_Interface()
    # data=[('1','2'),('3','4'),('5','6')]
    query = 'select * from user_info_table limit 100'
    res = mi.select_asQuery(query)
    print(res)
