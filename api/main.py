# click-event-big-data-app/api/main.py
"""
Bu modül, Click Event API'sinin ana giriş noktasıdır (entry point).
FastAPI uygulamasını oluşturur, endpoint'leri tanımlar ve gelen istekleri
diğer modüllere (modeller, servisler) yönlendirerek iş akışını yönetir.
"""

import logging
import os
import sys
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException

app_dir = os.path.dirname(os.path.abspath(__file__)) # /click-event-big-data-app/api
project_dir = os.path.dirname(app_dir) # /click-event-big-data-app
sys.path.append(project_dir)

from utils.logger import setup_logger
from .configs.api_configs import KAFKA_BOOTSTRAPSERVERS, KAFKA_TOPIC

setup_logger(name="api", appDir=app_dir)

from .models.event_models import ClickEvent

app = FastAPI(  
    title="Click Event Ingestion API",
    description="Kullanıcı etkileşim verilerini toplyıp Kafka'ya gönderen API",
    version="1.0.0"
)

@app.post("/click", status_code=202)
async def process_click_event(event: ClickEvent):
    """
    Bir click event'ini kabul eder, Pydantic ile doğrular ve işlenmek üzere
    arka plan sistemlerine (Kafka) gönderir.
    """
    LOG = logging.getLogger(__name__)
    LOG.info(f"Geçerli bir click event alındı. Device ID: {event.deviceId}")

    try:
        event_dict = event.model_dump()

        LOG.debug(f"Kafka'ya gönderilecek veri: {event_dict}")

        return {"status": "accepted"}
    except Exception as e:
        LOG.error(f"Click event işlenirken beklenmedik bir hata oluştu: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error while processing event.")

