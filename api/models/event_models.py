# click-event-big-data-app/api/models/event_models.py
"""
Bu modül, API tarafından kabul edilen veri yapılarını Pydantic modelleri
olarak tanımlar. Bu modeller, gelen verinin otomatik olarak doğrulanmasını,
dönüştürülmesini ve dokümante edilmesini sağlar.
"""

from pydantic import BaseModel, Field
from typing import Optional

class ClickLocation(BaseModel):
    """
    Açıklama:
        Tıklama olayının coğrafi konumunu temsil eden alt model.
    """
    lat: float = Field(..., description="Enlem bilgisi", examples=[41.0082])
    lon: float = Field(..., description="Boylam bilgisi", examples=[28.9784])
    city: str = Field(..., description="Şehir bilgisi", examples=["İstanbul"])

class ClickData(BaseModel):
    """
    Açıklama:
        Tıklama olayının koordinatları gibi detaylarını temsil eden alt model.
    """
    x: int = Field(..., description="Tıklama olayının X koordinatı.")
    y: int = Field(..., description="TIklama olayının Y koordinatı")
    element_id: Optional[str] = Field(None, description="Tıklanan HTML elementinin ID'si (opsiyonel)", examples=["buy-now"])

class ClickEvent(BaseModel):
    """
    Açıklama:
        Bir kullanıcı etkileşimini (click event) temsil eden ana veri modeli.
        Bu model, /click endpoint'ine gönderilecek olan JSON yapısını tanımlar.
    """
    deviceId: str = Field(..., description="Kullanıcının benzersiz cihaz ID'si. Genellikle bir cookie veya mobil ID'dir.", examples=["abc-123-def-456"])
    ip: str = Field(..., description="İsteğin yapıldığı IP adresi",examples=["88.255.10.10"])
    click: ClickData = Field(..., description="Tıklama olayının koordinatları ve detayları.")
    session: str = Field(..., description="Kullanıcının o anki ziyaretini (session) tanımlayan benzersiz ID.", examples=["sess_a1b2c3d4"])
    timestamp: int = Field(..., description="Olayın Unix zaman damgası (milisaniye cinsinden).", examples=[1678886400000])
    location: Optional[ClickLocation] = Field(None, description="Olayın coğrafi konumu (eğer varsa).")