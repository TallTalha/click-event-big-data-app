# click-event-big-data-app/api/services/kafka_producer.py
"""
Bu modül, Apache Kafka ile iletişimi yönetir ve Kafka Producer nesnesinin
yaşam döngüsünü (lifecycle) kontrol eder.
"""
import logging
import json
from kafka import KafkaProducer
from kafka.errors import KafkaError

LOG = logging.getLogger(__name__)

_producer: KafkaProducer | None = None

def startup_kafka_producer(bootstrap_server: str) -> None:
    """
    Açıklama:
        Uygulama başladığında çağrılır. Kafka'ya bağlanır ve global producer
        nesnesini başlatır. Global nesne, her istekte yeni bir bağlantı 
        oluşturmanın getireceği performans yükünü azaltır.
    Args:
        bootstrap_server(str): Kafka arcının bootstrap server adresi.
    Returns:
        None
    """
    global _producer
    LOG.info("Kafka Producer başlatılıyor...")
    try:
        _producer = KafkaProducer(
            bootstrap_servers=bootstrap_server,
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            acks="all", # Broker ve diğer tüm nodeların onayı beklenir.(Bilgi amaçlıdır, sunucumuz zaten single node)
            retries = 3, # Geçici hata durumlarında 3 kere tekrar göndermeyi dener
            linger_ms = 10 # GÖnderimler arası 10ms bekler, bu sayede batch şeklinde daha verimli veri gönderimi yapılabilir.
        )
        LOG.info("Kafka Producer, Kafka aracına başarıyla bağlandı.")
    except KafkaError as ke:
        LOG.critical(f"Kafka Producer, Kafka aracına bağlanırken hata oluştu: {ke}", exc_info=True)
        _producer = None

def shutdown_kafka_producer() -> None:
    """
    Açıklama:
        Uygulama kapandığında çağrılır. Producer'ı temiz bir şekilde kapatır.
    Args:
        None
    Returns:
        None
    """
    LOG.info("Kafka Producer kapatılıyor...")
    if _producer:
        try:
            _producer.flush(timeout=10000) # Tampondaki mesajların gönderilmesi için 10sn bekler
            _producer.close(timeout=10000)
            LOG.info("Kafka Producer başarıyla kapatıldı.")
        except KafkaError as ke:
            LOG.error(f"Kafka Producer kapatılırken hata oluştu: {ke}", exc_info=True)

async def send_event_to_kafka(topic: str, event_data: dict) -> bool:
    """
    Açıklama:
        Verilen bir olayı (event) belirtilen Kafka topic'ine gönderir.
    Args:
        topic (str): Mesajın gönderileceği Kafka topic'i.
        event_data (dict): Gönderilecek olan JSON verisi (Python sözlüğü).
    Returns:
        bool: Gönderme işleminin başarılı olup olmadığı.
    """
    if not _producer:
        LOG.warning(f"Kafka Producer aktif değil, '{topic}' topiğine mesaj gönderilemedi.")
        return False
    
    try:
        future = _producer.send(
            topic=topic,
            value=event_data
        )
        LOG.debug(f"Mesaj '{topic}' topic'ine gönderim için sıraya alındı.")
        return True
    except KafkaError:
        LOG.error(f"'{topic}' topiğine mesaj gönderilirken hata oluştu.", exc_info=True)
        return False