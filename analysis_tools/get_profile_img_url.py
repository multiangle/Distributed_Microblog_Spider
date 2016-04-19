__author__ = 'multiangle'


from pymongo import MongoClient

client=MongoClient('localhost',27017)
db=client['microblog_spider']
latest_history=db.latest_history
res=latest_history.find({'user.profile_image_url':{'$ne':None}})
res= [x for x in res]
print(res.__len__())

