from sofa import Communicator, RedisHandler
import json

def subtract(data):
    initial_val = data['value']
    value = initial_val - 4
    data['value'] = value
    broker.publish(data, 'subtracted')

broker = Communicator('subtract', 'multiplied')
broker.listen(subtract)
