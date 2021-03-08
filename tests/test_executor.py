import asyncio
import time
from pipegram.executor import HybridPoolExecutor


def func():
    print('func start')
    time.sleep(1)
    print('func end')
    return 'hello world'


async def func1():
    print('func1 start')
    await asyncio.sleep(2)
    print('func1 end')


async def func2():
    print('func2 start')
    await asyncio.sleep(2)
    print('func2 end')


async def async_main():
    executor = HybridPoolExecutor()
    future1 = executor.submit(func1)
    future2 = executor.submit(func2)
    print(future1.get())
    print(future2.get())


if __name__ == '__main__':
    asyncio.run(async_main())
