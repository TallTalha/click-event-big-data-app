# configs/settings.py
"""
Bu modül, uygulama için gerekli ayarları ve API anahtarlarını içerir.
Ayarlar, çevresel değişkenlerden alınır ve uygulama genelinde kullanılabilir.
"""
import os
from dotenv import load_dotenv

load_dotenv()

# Kafka Consumer Parametreleri
KAFKA_BOOTSTRAPSERVERS = str(os.getenv("KAFKA_BOOTSTRAPSERVERS"))
KAFKA_TOPIC = str(os.getenv("KAFKA_TOPIC"))
CONSUMER_GROUP_ID = "elasticsearch-indexer-group-1"


# Elastich Search Bağlantı Ayarları
ELASTIC_HOST = str(os.getenv('ELASTIC_HOST'))
ELASTIC_PORT = str(os.getenv('ELASTIC_PORT'))
ELASTIC_URL = f"https://{ELASTIC_HOST}:{ELASTIC_PORT}"
ELASTIC_USER = str(os.getenv('ELASTIC_USER'))
ELASTIC_PASSWORD =  str(os.getenv('ELASTIC_PASSWORD'))
ELASTIC_FINGERPRINT = str(os.getenv('ELASTIC_FINGERPRINT'))

# Elastich Search Parametreleri
ELASTIC_INDEX_NAME = "click-events"
BATCH_SIZE = 500  
BATCH_TIMEOUT_SECONDS = 10 
