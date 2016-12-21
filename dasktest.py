from os import environ as e
from sofa import Communicator, Database, RedisHandler
from subprocess import run
import json
import csv
import time

def delay():
#    sub = e['SUBSCRIBE_KEY']
    while True:
       try:
           comm = Communicator('test', 'dummy')
           print("CONNECTION SUCCESSFUL")
       except:
           continue
       else:
           break

#def perpetual_services(junk):
#    run(['docker-compose', '-f', 'perpetual_services.yml', 'up', '-d'])
#    print('started')
#    return ''

def load_csv(filepath):
    print('load')
    contents = {}
    with open(filepath) as csvfile:
        reader = csv.DictReader(csvfile)
        for n, row in enumerate(reader):
            contents[str(n)] = row
    return contents

#def microservices(input):
#    run(['docker-compose', '-f', 'microservices.yml', 'up', '-d'])
#    return 'MOCK_DATA.csv'

def clean_send(data, pubkey):
#    microservices()
    delay()
    broker = Communicator('send', 'first')
#    info = data.pop([0])
    for key in data.keys():
#        body = { info[0]: row[0], info[1]: int(row[1]) }
        broker.publish(data[key], pubkey)
#        body.clear()
    print('sent')
    return ''

def retrieval(input):
    print('retrieval')
    db = RedisHandler('10.128.0.2')
    keys = db.keys()
    while len(keys) != 1000:
        print(len(keys))
        time.sleep(1)
        keys = db.keys()
    keys.sort(key=int)
    m = []
    for key in keys:
        m.append([eval(key), eval(db.get(key))])
    with open('MOCK_RESULT.csv', 'w') as csvfile:
        fieldnames = ['id', 'value']
        writer = csv.writer(csvfile)
        writer.writerow(fieldnames)
        for row in m:
            writer.writerow(row)



#dsk = {'startup': (perpetual_services, None),
#       'microservices': (microservices, 'startup'),
dsk = {'load_csv': (load_csv, 'microservices'),
       'clean_send': (clean_send, 'load_csv', 'first'),
       'retrieval': (retrieval, 'clean_send')}

#from dask.multiprocessing import get
#get(dsk, 'retrieval')

from distributed import Client
client = Client('10.128.0.2:8786')
client.get(dsk, 'retrieval')
