# click-event-big-data-app/consumer/run_es_consumer.py
"""
Bu modül, bir Kafka topiğini dinleyen ve gelen mesajları toplu olarak
Elasticsearch'e yazan bir tüketici (consumer) servisinin iş akışını yönetir.
"""
import logging
import os
import sys
from time import sleep
from elasticsearch.helpers import bulk

current_dir = os.path.dirname(os.path.abspath(__file__)) # click-event-big-data-app/consumer/
project_root = os.path.dirname(current_dir) # click-event-big-data-app/
sys.path.append(project_root)

from utils.logger import setup_logger
from .configs.consumer_configs import (
    KAFKA_BOOTSTRAPSERVERS, KAFKA_TOPIC, 
    ELASTIC_URL, ELASTIC_USER, ELASTIC_PASSWORD, ELASTIC_FINGERPRINT, CONSUMER_GROUP_ID,
    ELASTIC_INDEX_NAME, BATCH_SIZE, BATCH_TIMEOUT_SECONDS
) # KAFKA-ELASTICHSEARCH bağlantı ayarları ve KAFKA-ELASTICHSEARCH veri gönderim parametreleri
from .es_consumer_funcs import connect_to_kafka, connect_to_es, _create_es_actions

setup_logger(appDir=current_dir, name="es_consumer")
LOG = logging.getLogger(__name__)

def main():
    """
    Ana iş akışını yönetir.
    """
    LOG.info("Kafka'dan Elasticsearch'e veri aktarma servisi başlatılıyor...")

    es_client = connect_to_es(elastic_url=ELASTIC_URL, elastic_user=ELASTIC_USER, elastic_paswd=ELASTIC_PASSWORD, elastic_fingerprint=ELASTIC_FINGERPRINT)
    if not es_client:
        LOG.info("Elastichsearch istemcisi oluşmadı. İşlem sonlandırıldı.")
        sys.exit(1) # ÇIKIŞ -> ES istemcisi oluşması.

    while True:
        kafka_consumer = connect_to_kafka(kafka_server=KAFKA_BOOTSTRAPSERVERS, kafka_topic=KAFKA_TOPIC, group_id=CONSUMER_GROUP_ID)
        if not kafka_consumer:
            LOG.warning("Kafka bağlantısı kurulamadı. 15 saniye sonra tekrar denenecek.")
            sleep(15)
            continue
        
        message_batch = []
        try:
            LOG.info(f"{KAFKA_TOPIC}Topiğinden Mesajlar bekleniyor...")
            for message in kafka_consumer:
                message_batch.append(message)

                if len(message_batch) >= BATCH_SIZE:
                    
                    LOG.info(f"{len(message_batch)} adetlik batch doldu, Elasticsearch'e yazılıyor...")
                    success, errors = bulk(client=es_client, actions=_create_es_actions(message_batch, index_name=ELASTIC_INDEX_NAME))
                    
                    if errors:
                        LOG.warning(f"Bulk yazma sırasında {errors} adet hata oluştu.")
                    
                    message_batch = [] # Batch'i temizle
        except Exception as e:
            LOG.error(f"Tüketici döngüsünde bir hata oluştu: {e}. Yeniden bağlanılacak...", exc_info=True)
            if kafka_consumer:
                kafka_consumer.close()
            sleep(BATCH_TIMEOUT_SECONDS)

if __name__ == "__main__":
    main()



