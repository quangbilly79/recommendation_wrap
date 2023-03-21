import hashlib

def md5(text: str):
    hashlib.md5(text.encode()).hexdigest()