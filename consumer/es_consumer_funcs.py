# click-event-big-data-app/consumer/es_consumer_funcs.py
"""
Bu modül, bir Kafka topiğini dinleyen ve gelen mesajları toplu olarak
Elasticsearch'e yazan bir tüketici (consumer) servisi için gerekli fonksiyonları içerir.
    - connect_to_kafka()
    - connect_to_es()
    - _create_es_actions
"""
import logging
import json

from kafka import KafkaConsumer
from kafka.errors import KafkaError
from elasticsearch import Elasticsearch

LOG = logging.getLogger(__name__)

def connect_to_kafka( kafka_server: str ,kafka_topic: str, group_id: str, startingOffsets: str = 'earliest') -> KafkaConsumer | None:
    """
    Açıklama:
        Kafka Server ve Kafka Topic  kullanılarak KafkaConsumer nesnesi döndürülür.
    Args:
        kafka_server(str): Consume edilmesi gereken kafka bootstrap server adresi.  
        kafka_topic(str): Consume edilmesi gereken kafka topiğinin adıdır.
        group_id(str): Kafka'nın kendisinin topiğin neresinde kalındığı takip etmesi için verilen grup ismidir.
        startingOffsets(str): (earliest|latest|<specific_partition>) Topiğin neresinden verilerin okunmaya başlaması gerektiğini belirtir. 
    Returns:
        kafka_consumer(KafkaConsumer|None): Hata oluşmazsa, girdilere göre ayarlanmış Kafka Consumer nesnesi, bağlantı hatası oluşursa None dönder.
    """
    LOG.info(f"{kafka_topic} topiğinden veriler okunuyor...")
    try:
        kafka_consumer = KafkaConsumer(
            kafka_topic,
            bootstap_servers = kafka_server,
            group_id = group_id,
            auto_offset_reset = startingOffsets,
            value_deserializer = lambda v: json.loads(v.decode("utf-8"))
        )
        LOG.info("Kafka Consumer başarıyla bağlandı.")
        return kafka_consumer
    except KafkaError as ke:
        LOG.critical(f"Kafka'ya bağlanılamadı : {ke}", exc_info=True)
        return None
    
def connect_to_es(elastic_url: str, elastic_user: str, elastic_paswd: str, elastic_fingerprint: str) -> Elasticsearch | None:
    """
    Açıklama:
        Elasticsearch'e bağlanır ve  istemci nesnesini döndürür.
    Args:
        elastic_url(str): ´https://{ELASTIC_HOST}:{ELASTIC_PORT}´ formatındaki URL.
        elastic_user(str): Elastichsearch aracında kullanılacak kullanıcı adı.
        elastic_paswd(str): Elastichsearch aracında kullanılacak kullanıcı şifresi.
        elastic_fingerprint(str): Güvenli bağlantı için ´CD:EF:...:34:56´ formatındaki parmak izi.
    Returns:
        es_client(Elasticsearch|None): Bağlantı hatası oluşmazsa ES istemcisi, hata oluşursa None döner.
    """
    LOG.info("Elastichsearch bağlantısını sağlanıyor...")
    try:
        es_client = Elasticsearch(
            hosts=[elastic_url],
            basic_auth=(elastic_user, elastic_paswd),
            ssl_assert_fingerprint=elastic_fingerprint,
            request_timeout=60
        )
        if es_client.ping():
            LOG.info("ping() ile bağlantı test edildi: Elastichsearch **bağlantısı başarılı**.")
            return es_client
        else:
            LOG.error("ping() ile bağlantı test edildi: Elastichsearch **bağlantısı başarısız**.")
            return None
    except Exception as e:
        LOG.critical(f"Elastichsearch bağlantısı sırasında hata: {e}", exc_info=True)
        return None
    
def _create_es_actions(messages: list, index_name: str):
    """
    Açıklama:
        Kafka'dan gelen mesajları, Elasticsearch bulk API formatına dönüştürür. Elastichsearch ´bulk´ fonksiyonu için yardımcı generator fonksiyon görevi görür.
    Args:
        messages(list): bulk API formatına çevrilecek olan veri listesi.
        index_name(str): Verilerin yazılacağı Elastichsearch index ismi.
    """
    for msg in messages:
        doc_id =  f"{msg.value.get('deviceId')}-{msg.value.get('session')}-{msg.value.get('timestamp')}"

        yield {
            "_index": index_name,
            "_id": doc_id,
            "_source": msg.value
        }

