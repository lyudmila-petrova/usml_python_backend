## Определение страны по ip
Используется библиотека:
https://pypi.org/project/python-geoip-python3/ 

Устанавливается командой `pip install python-geoip-python3`

База данных скачана с официального сайта:
https://dev.maxmind.com/geoip/geoip2/geolite2/

#### Пример использования
```python
from geoip import open_database
db = open_database('GeoLite2/GeoLite2-Country.mmdb')
res = db.lookup('15.227.66.159')
print(res.country)
```

#### Документация
https://python-geoip-yplan.readthedocs.io/en/latest/


