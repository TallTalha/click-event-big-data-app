# utils/logger.py
"""
Bu modül, her uygulamanın genelinde kullanılacak logger'ı ayarlar.
Logger, hem konsola hem de dosyaya loglama yapar.
Log dosyaları, her uygulamanın kök dizinindeki logs klasöründe saklanır.
Loglama formatı, tarih, modül adı, log seviyesi, thread ve mesajı içerir.
Log dosyaları, 10 MB boyutuna ulaştığında yeni bir dosya oluşturur ve en fazla 5 yedek dosya tutar. 
"""
import logging
import logging.handlers
import os
import sys
import re

def setup_logger(name: str, appDir: str, level: int = logging.INFO) -> None:
    """
    Açıklama:
        appDir dizini altında logs klasörü oluşturur. logs klasörü altında, yedekleme özelliği, konsola yazma özelliği
        eklenmiş name.log dosyası oluşturur. 
            Yedekleme Özelliği:                     
                RotatingFileHandler ile log 10 MB boyutuna ulaştığında yeni bir dosya oluşturur, en fazla 5 yedek dosya tutar.
    Args:
        name (str): Log dosya adı, genellikle modül adı olarak kullanılır.
        appDir (str): logs klasörünün oluşturulacağı uygulama dizini.
        level (int): Log seviyesini belirler. Varsayılan olarak INFO seviyesidir.
    Returns:
        None
    """
    LOGS_FILE_PATH = os.path.join(appDir,"logs") 
    os.makedirs(LOGS_FILE_PATH, exist_ok=True) # app dizini altında logs klasörü yoksa oluşturulur.

    safe_name = re.sub(r"[^\w\-_.]", "_", name)
    LOG_FILE = os.path.join(LOGS_FILE_PATH, f"{safe_name}.log")

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - [%(threadName)s] - %(message)s")

    fileHandler = logging.handlers.RotatingFileHandler(
        LOG_FILE, maxBytes=10*1024*1024, backupCount=5
    ) 
    fileHandler.setFormatter(formatter)
    root_logger.addHandler(fileHandler)

    consoleHandler = logging.StreamHandler(sys.stdout)
    consoleHandler.setFormatter(formatter)
    root_logger.addHandler(consoleHandler)