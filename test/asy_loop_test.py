
import asyncio
import threading
import time

async def asy_test(id,gap):
    count = 0
    while True:
        count += 1
        await asyncio.sleep(gap)
        print('id:{i}, count: {c}'.format(i=id,c=count))

class task_ctl(threading.Thread):
    def __init__(self,tasks):
        self.tasks = tasks
        threading.Thread.__init__(self)

    def run(self):
        time.sleep(5)
        print('ready to add a task')
        self.tasks.append(asy_test(4,2))
        time.sleep(5)
        print('ready to sub a task')
        self.tasks.pop(0)

loop = asyncio.get_event_loop()
tasks = [asy_test(i,2) for i in range(3)]
t = task_ctl(tasks)
t.start()
loop.run_until_complete(asyncio.wait(tasks))
loop.close()