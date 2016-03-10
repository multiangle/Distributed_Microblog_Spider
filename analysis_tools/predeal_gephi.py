__author__ = 'multiangle'

from DB_Interface import MySQL_Interface
import json
import networkx as nx

dbi=MySQL_Interface()

# create table (select * from user_info_table order by fans_num limit 1000)

[web_info,col_info]=dbi.select_all('temp_table2')
select_web=[]
select_user={}
for atte in web_info:
    if (atte[1],atte[0]) in web_info:
        select_web.append(list(atte))
        select_user[atte[1]]=1
        select_user[atte[0]]=1
select_user=select_user.keys()

G=nx.Graph()
G.add_nodes_from(select_user)
G.add_edges_from(select_web)
nx.write_gexf(G,'weibo_node1000.gexf')