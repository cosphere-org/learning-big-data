
# from exercise import bytes_to_str, str_to_bytes
from answer import bytes_to_str, str_to_bytes


def test_bytes_to_str():
    assert bytes_to_str(b'hi there') == 'hi there'
    assert bytes_to_str(b'cze\xc5\x9b\xc4\x87') == 'cześć'
    

def test_str_to_bytes():
    assert str_to_bytes('hi there') == b'hi there'
    assert str_to_bytes('dziękuję') == b'dzi\xc4\x99kuj\xc4\x99'
    
    # -- length is 
    assert len(str_to_bytes('dziękuję')) == 10