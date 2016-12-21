from sofa import Communicator, RedisHandler
import json

def add(data):
    initial_val = int(data['value'])
    value = initial_val + 5
    data['value'] = value
    broker.publish(data, 'added')

broker = Communicator('add', 'first')
broker.listen(add)
