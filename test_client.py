import asyncio
import aiohttp
import random
import time
import logging

from core import USERNAMES, SERVER_PORT, LOGLEVEL

logger = logging.getLogger()

async def requester(username: str, requests: int):
    body = {'username': username, 'text': 'text'}
    s_t = time.time()
    async with aiohttp.ClientSession() as session:
        for _ in range(requests):
            try:
                async with session.post(f'http://127.0.0.1:{SERVER_PORT}/message', json=body) as resp:
                    resp.raise_for_status()
                    logger.debug(await resp.json())
            except Exception as ex:
                logger.error(ex)
    
    total_t = time.time() - s_t
    
    return requests / total_t

async def start_testing(clients: int = 50, requests: int = 100):
    tasks = []
    s_t = time.time()
    
    for _ in range(clients):
        username = random.choice(USERNAMES)
        
        t = asyncio.create_task(requester(username, requests))
        tasks.append(t)
        
    clients_res = await asyncio.gather(*tasks)
    res_time = time.time() - s_t
    
    logger.info(f"TOTAL TIME: {res_time}")
    logger.info(f"AVG requests per sec: {sum(clients_res) / len(clients_res)}")
    

def _run():
    handler = logging.FileHandler('client.log')
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    
    logger.addHandler(handler)
    logger.setLevel(LOGLEVEL)
    
    asyncio.run(start_testing(50))

if __name__ == '__main__':
    _run()
    