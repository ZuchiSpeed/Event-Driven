import json

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from redis_om import get_redis_connection, HashModel

import consumers


app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=['http://localhost:3000'],
    allow_methods=['*'],
    allow_headers=['*']
)

redis = get_redis_connection(
    host="",
    port=19295,
    password="",
    decode_responses=True
)

class Delivery(HashModel):
    budget: int = 0
    notes: str = ""
    
    class Meta:
        database = redis
        
class Event(HashModel):
    delivery_id: str = ""
    type: str
    data: str
    
    class Meta:
        database = redis
  
#If Id is available get Id else load empty object        
@app.get('deliveries/{pk}/status')
async def get_status(pk: str):
    #product key from radis
    state = redis.get(f'delivery: {pk}')
    
    if state is not None:
        return json.loads(state)
    
    return {}

#Rebuilding a state if we didn't get a product key from Radis
def build_state(pk: str):
    pks = Event.all_pks()
    all_event = [Event.get(pk) for pk in pks]
    events = [event for event in all_event if event.delivery_id == pk]
    state = {}
    
    for event in events:
        state = consumers.CONSUMERS[event.type](state, event)
        
    return state
        
#Create a Post request which creates a new Delivery object and Event object, store Id in the redis chache
@app.post('/deliveries/create')
async def create(request: Request):
    body = await request.json()
    delivery = Delivery(budget=body['data']['budget'], notes=body['data']['notes']).save()
    event = Event(delivery_id=delivery.pk, type=body['type'], data=json.dumps(body['data'])).save()
    state = consumers.CONSUMERS[event.type]({}, event)
    redis.set(f'delivery:{delivery.pk}', json.dumps(state))
    return state

#Post an Event object which returns an updated state sent by the Id
@app.post('/event')
async def event(request: Request):
    body = await request.json()
    delivery_id = body['delivery_id']
    event = Event(delivery_id=body['delivery_id'], type=body['type'], data=json.dumps(body['data'])).save()
    state = await get_status(delivery_id)
    new_state = consumers.CONSUMERS[event.type](state, event)
    redis.set(f'delivery:{delivery_id}', json.dumps(new_state))
    return new_state