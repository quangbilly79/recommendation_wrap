# Config kết nối đến Redis và mySQL
REDIS_SERVICE = 'vgdata'
SENTINEL_CONFIGS = [
    ("172.25.0.109", 50000), ("172.25.0.102", 50000), ("172.25.0.110", 50000)
]
MYSQL_SERVER = {
    "host": "172.25.0.101",
    "user": "etl",
    "password": "Vega123312##",
    "database": "waka"
}