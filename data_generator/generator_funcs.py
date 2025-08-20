# click-event-big-data-app/data_generator/generator_funcs.py
"""
Bu modül, click event simülasyonu için gerekli olan
sahte veri üretme fonksiyonlarını barındırır.
"""
import random
import uuid
import time
from faker import Faker

faker = Faker("tr_TR")

city_coords = {
    "İstanbul": (41.0082, 28.9784),
    "Ankara": (39.9208, 32.8541),
    "İzmir": (38.4192, 27.1287),
    "Konya": (37.8715, 32.4846),
    "Gaziantep": (37.0662, 37.3833),
    "Antalya": (36.8969, 30.7133),
    "Van": (38.4945, 43.3832),
    "Trabzon": (41.0015, 39.7178)
}
citiesW = [40,20,15,10,5,5,1,4]  
buttons = ["login", "Siparislerim", "GununFirsatlari", "EgitimDefter"]

def generate_click_event() -> dict:
    """
    Açıklama:
        Tek bir sahte click event sözlüğü üretir.
    Args:
        None
    Returns:
        clickEvent(dict): {deviceId, ip, click(x, y, element_id), session, timestamp, location(lat, lon)} key değerlerine sahip bir click event sözlüğü döner.
    """
    chosen_city = random.choices(list(city_coords.keys()), weights=citiesW, k=1)
    lat, lon = city_coords[chosen_city[0]]
    lat = lat + random.uniform(-0.1, 0.1) # Şehrin merkezi üzerinde yaklaşık 10 km yarı çapındaki 
    lon = lon + random.uniform(-0.1, 0.1) # daire üzerinde rastgele koordinat seçimi yapılır

    clickEvent = {
        "deviceId" : str(uuid.uuid4()),
        "ip" : faker.ipv4(),
        "click" : {
            "x" : random.randint(1, 1920),
            "y" : random.randint(1, 1080),
            "element_id" : random.choice(buttons)
        },
        "session" : str(uuid.uuid4()),
        "timestamp" : int(time.time() * 1000),
        "location" : {
            "lat" : float(lat),
            "lon" : float(lon),
            "city" : str(chosen_city[0])
        }
    }

    return clickEvent