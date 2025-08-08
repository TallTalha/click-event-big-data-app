# configs/settings.py
"""
Bu modül, uygulama için gerekli ayarları ve API anahtarlarını içerir.
Ayarlar, çevresel değişkenlerden alınır ve uygulama genelinde kullanılabilir.
"""
import os
from dotenv import load_dotenv

load_dotenv()

KAFKA_BOOTSTRAPSERVERS = str(os.getenv("KAFKA_BOOTSTRAPSERVERS"))
KAFKA_TOPIC = str(os.getenv("KAFKA_TOPIC"))
