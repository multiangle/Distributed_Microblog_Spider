__author__ = 'multiangle'
import time

def formate_time(timestamp):
    print(time.strftime('%Y-%m-%d %H:%M',time.localtime(timestamp)))

if __name__=='__main__':
    formate_time(1461092302)