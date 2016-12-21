from os import environ as e
from sofa import Communicator, Database, RedisHandler
from dask.multiprocessing import get
from subprocess import call

def delay():
    sub = e['SUBSCRIBE_KEY']
    while True:
       try:
           comm = Communicator(sub)
           print("CONNECTION SUCCESSFUL")
       except:
           continue
       else:
           break

def perpetual_services():
    subprocess.call('docker-compose up', shell=True)
    delay()

def load_csv(filepath):
    contents = {}
    with open(filepath) as csvfile:
        reader = csv.DictReader(csvfile)
        n = 0
        for row in reader:
            contents[str(n)] = row
            n += 1
    return contents

def clean_send(data, pubkey):
    broker = Communicator(None)
#    info = data.pop([0])
    for key in data:
#        body = { info[0]: row[0], info[1]: int(row[1]) }
        broker.publish(data[key], pubkey)
#        body.clear()

def add5(x):
    return x + 5

def mult2(x):
    return x * 2

def subtract4(x):
    return x - 4

def operation(data):
    dset = json.loads(data)
    initial_val = dset['value']
    value = subtract4(mult2(add5(initial_val)))
    dset['value'] = value
    msg = dset.dumps(dset)
    broker.publish(msg, 'second')

def run_listener(queue, sub):
    broker = Communicator(queue, sub)
    broker.listen(operation)

def set_hack(data):
    db = RedisHandler()
    dset = json.loads(data)
    key = data['id']
    value = data['value']
    db.set(key, value)

def redis_store(queue, sub):
    broker = Communicator(queue, sub)
    broker.listen(set_hack)

def service_starter():


def retrieval():
    db = RedisHandler()
    keys = db.keys()
    keys.sort()
    values = []
    for key in keys:
        val = db.get(key)
        values.append(val)
    with open('MOCK_RESULT.csv') as csvfile:
        fieldnames = ['id', 'value']
        writer = csv.Dictwriter(csvfile, fieldnames=fieldnames)
        for row in writer:
            writer.writerow(row)
