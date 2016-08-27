
import asyncio
import threading
import time

async def asy_test_basic(id,gap):
    count = 0
    for i in range(5):
        count += 1
        await asyncio.sleep(gap)
        # print('hehe')
        print('id:{i}, count: {c}'.format(i=id,c=count))

loop = asyncio.get_event_loop()
tasks = [asy_test_basic(i,5) for i in range(5)]
loop.run_until_complete(asyncio.wait(tasks))
loop.close()




