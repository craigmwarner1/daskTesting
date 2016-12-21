from sofa import Communicator, RedisHandler
import json

def collect(data):
    iden = data['id']
    value = data['value']
    db = RedisHandler('10.128.0.2')
    db.set(iden, value)

broker = Communicator('collect', 'subtracted')
broker.listen(collect)
