import asyncio

async def example(n):
    return [i for i in range(n)]

async def main():
    for i in range(10):
        a = await example(i)
        print(a)

loop = asyncio.get_event_loop()

loop.run_until_complete(main())
loop.close()