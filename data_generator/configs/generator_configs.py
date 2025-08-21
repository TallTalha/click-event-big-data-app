# configs/settings.py
"""
Bu modül, uygulama için gerekli ayarları ve API anahtarlarını içerir.
Ayarlar, çevresel değişkenlerden alınır ve uygulama genelinde kullanılabilir.
"""
import os
from dotenv import load_dotenv

load_dotenv()

API_URL = str("http://localhost:8080/click")
NUM_EVENTS = int(10000)
CONCURRENCY_LIMIT = int(100) # Aynı anda en fazla 100 istek

