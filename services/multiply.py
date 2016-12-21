from sofa import Communicator, RedisHandler
import json

def multiply(data):
    initial_val = data['value']
    value = initial_val * 2
    data['value'] = value
    broker.publish(data, 'multiplied')

broker = Communicator('multiply', 'added')
broker.listen(multiply)
