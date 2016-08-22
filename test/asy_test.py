
import asyncio
import aiohttp

async def asy_test(url, id, proxy_used=0):
        try:
            res = await singleConn(url,id)
            print(res)
            return res
        except Exception as e:
            if proxy_used<3:
                print('{i} current proxy invalid ,switch another'.format(i=id))
                return await asy_test(url, id, proxy_used+1)
            else:
                # raise RuntimeError('2333333333333')
                print('2333333333333')

async def singleConn(url,id,times=0):
    headers = {'User-Agent': 'Mozilla/5.0 (iPhone; CPU iPhone OS 8_0 like Mac OS X) '
                             'AppleWebKit/600.1.3 (KHTML, like Gecko) Version/8.0 Mobile'
                             '/12A4345d Safari/600.1.4'}
    async with aiohttp.ClientSession() as session :
        try:
            with aiohttp.Timeout(3):
                async with session.get(url,headers=headers) as resp:
                    content = await resp.read()
            content = content.decode('utf8')
            print(content)
            return content
        except Exception as e:
            print(e)
            print('{i} try again {x}'.format(x=times,i=id))
            if times<3:
                return await singleConn(url,id,times+1)
            else:
                raise RuntimeError('{i} This proxy cannot connect , swith another'.format(i=id))




loop = asyncio.get_event_loop()
url = 'http://127.0.0.1:8080/test'
tasks = [asy_test(url,i) for i in range(10)]
loop.run_until_complete(asyncio.wait(tasks))
loop.close()
