# click-event-big-data-app/data_generator/api_client.py
"""
Bu modül, Click Event API'sine asenkron olarak istek gönderme
sorumluluğunu üstlenir.
"""
import asyncio
import httpx
import logging

LOG = logging.getLogger(__name__)

async def send_event(client: httpx.AsyncClient, api_url: str, event_data: dict, event_num: int):
    """
    Açıklama:
        Tek bir event'i API'ye asenkron olarak gönderir.
    Args:
        client(AsyncClient): Asenkron gönderim için gerekli istemci nesnesi.
        api_url(str): API depolama adresi.
        event_data(dict): Gönderilecek olan event sözlüğü.
        event_num(int): Gönderilecek her bir veri için oluşturulan tam sayı değeri.
    Returns:
        None    
    """
    try:
        response = await client.post(api_url, json=event_data, timeout=10)
        response.raise_for_status()  # HTTP 4xx veya 5xx hatası varsa, uyarı fırlatır

        if (event_num+1) % 500 == 0:
            LOG.info(f"{event_num+1} adet event başarıyla gönderildi.")
    except httpx.HTTPStatusError as se:
        LOG.error(f"#{event_num + 1} gönderilirken HTTP hatası: {se.response.status_code} - {se.response.text}")
    except httpx.RequestError as re:
        LOG.error(f"Event #{event_num + 1} gönderilirken ağ hatası: {re}")

async def send_event_with_semaphore(semaphore: asyncio.Semaphore, client: httpx.AsyncClient, api_url: str, event_data: dict, event_num: int):
    """
    Açıklama:
        Semaphore kullanarak send_event'i güvenli bir şekilde çağırır.
    Args:
        semaphore(Semaphore): Semaphore ile güvenli gönderim için gerekli asyncio kütüphane nesnesi.
        client(AsyncClient): Asenkron gönderim için gerekli istemci nesnesi.
        api_url(str): API depolama adresi.
        event_data(dict): Gönderilecek olan event sözlüğü.
        event_num(int): Gönderilecek her bir veri için oluşturulan tam sayı değeri.
    Returns:
        None    
    """
    async with semaphore:
        await send_event(client, api_url, event_data, event_num)