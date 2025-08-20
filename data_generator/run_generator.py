# click-event-big-data-app/data_generator/run_generator.py
import logging
import asyncio
import httpx
import os
import sys

app_dir = os.path.dirname(os.path.abspath(__file__))
project_dir = os.path.dirname(app_dir)
sys.path.append(project_dir)

from utils.logger import setup_logger
setup_logger(name="data_generator", appDir=app_dir)

LOG = logging.getLogger(__name__)


from .generator_funcs import generate_click_event
from .api_client import send_event_with_semaphore

from .configs.generator_configs import API_URL, NUM_EVENTS, CONCURRENCY_LIMIT

async def main():
    """
    Ana asenkron iş akışını yönetir.
    """
    LOG.info(f"'{API_URL}' adresine {NUM_EVENTS} adet sahte click event gönderimi başlıyor...")
    async with httpx.AsyncClient() as client:
        semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)
        tasks = []
        for i in range(NUM_EVENTS):
            event = generate_click_event()

            task = asyncio.create_task(
                send_event_with_semaphore(
                    semaphore=semaphore, client=client, api_url=API_URL, event_data=event, event_num=i
                )
            )
            tasks.append(task)
        
        await asyncio.gather(*tasks)
    
    LOG.info(f"{NUM_EVENTS} event'in tamamı için gönderme işlemi tamamlandı.")

if __name__ == "__main__":
    asyncio.run(main())