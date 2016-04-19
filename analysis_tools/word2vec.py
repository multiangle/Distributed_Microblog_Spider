__author__ = 'multiangle'

import jieba
import File_Interface as FI

data = FI.load_pickle('demo.pkl')
user_list = [x['user_name'] for x in data]
text_list = [x['dealed_text']['left_content'] for x in data]

for line in text_list:
    print(line)
    res = jieba.cut(line[0],cut_all=False)
    # print(list(seg_list))
    res = list(res)
    print(res)


