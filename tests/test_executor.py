import asyncio
import time
from pipegram.executor import HybridPoolExecutor


def func():
    print('func start')
    time.sleep(1)
    print('func end')
    return 'hello world'


async def async_main():
    executor = HybridPoolExecutor()
    future = executor.submit(func)
    print(await future.get_async())


if __name__ == '__main__':
    asyncio.run(async_main())
