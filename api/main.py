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
from .services.kafka_producer import startup_kafka_producer, send_event_to_kafka, shutdown_kafka_producer

@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Açıklama:
        Uygulama başladığında Kafka Producer'ı başlatır,
        uygulama kapandığında ise temiz bir şekilde kapatır.
    """

    # Uygulama başlarken çalışacak kod
    startup_kafka_producer(KAFKA_BOOTSTRAPSERVERS)
    yield # Bu noktada, uygulama gelen istekleri kabul etmeye başlar.
    # Uygulama kapanırken (örn: Ctrl+C ile durdurulduğunda) çalışacak kod
    shutdown_kafka_producer()


app = FastAPI(  
    title="Click Event Ingestion API",
    description="Kullanıcı etkileşim verilerini toplyıp Kafka'ya gönderen API",
    version="1.0.0",
    lifespan=lifespan
)
LOG = logging.getLogger(__name__)


@app.post("/click", status_code=202)
async def process_click_event(event: ClickEvent):
    """
    Gelen click event'ini alır, Pydantic ile doğrular ve Kafka'ya gönderir.
    """
    LOG.info(f"Geçerli bir click event alındı. Device ID: {event.deviceId}")

    try:
        event_dict = event.model_dump()

        # Artık Kafka'ya gönderme işlemini, servisimizdeki fonksiyona havale ediyoruz.
        success = await send_event_to_kafka(KAFKA_TOPIC, event_dict)
        
        if not success:
            # Eğer producer aktif değilse, servis fonksiyonu False dönecek.
            raise HTTPException(status_code=503, detail="Mesajlaşma servisi şu anda kullanılamıyor.")

        return {"status": "accepted"}

    except Exception as e:
        LOG.error(f"Click event işlenirken beklenmedik bir hata oluştu: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error while processing event.")

