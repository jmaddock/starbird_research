import os

def write_to_data(path=''):
    result = os.path.join(os.path.dirname(__file__),os.pardir,'data/') + path
    return result
