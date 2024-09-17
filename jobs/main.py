import os
from confluent_kafka import SerializingProducer
import simplejson as json
import datetime
from datetime import timedelta, datetime
import random
import uuid #universally unique ids across distributed systems. Ensuring uniqueness in context


LONDON_COORDINATES = {"latitude": 51.5074, "longitude": -0.1278}
BIRMINGHAM_COORDINATES = {"latitude": 52.4862, "longitude": -1.8904}


# Calculate movement increments
LATITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['latitude'] - LONDON_COORDINATES['latitude']) / 100
LONGITUDE_INCREMENT = (BIRMINGHAM_COORDINATES['longitude'] - LONDON_COORDINATES['longitude']) / 100


#Environment Variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092') #The location of the broker that kafka client will use to establish broker addresses. 
VEHICLE_TOPIC = os.getenv('VEHIVCLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv('GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv('TRAFFIC_TOPIC', 'traffic_data')
EMERGENCY_TOPIC = os.getenv('EMERGENCY_TOPIC', 'emergency_data')


start_time = datetime.now()
start_location = LONDON_COORDINATES.copy()



def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return {
        'id': uuid4(),
        'device_id': device_id,
        'time_stamp': timestamp,
        'speed': random.uniform(0,40),
        'direction': 'North-East',
        'vehicle_type': vehicle_type
    }


def generate_traffic_camera_data(device_id, time_stamp, camera_id):
    return {
        'id': uuid4,
        'device_id': device_id,
        'camera_id': camera_id,
        'time_stamp': time_stamp
        'snapshot': 'Base64EncodedString'
    }
    



def simulate_vehicle_movement():
    global start_location 
    
    #move towards birmingham
    start_location['latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT
    
    
    #randomnes to simulate actual travel
    start_location['latitude'] += random.uniform(-0.0005, 0.0005)
    start_location['longitude'] += random.uniform(-0.0005, 0.0005)
    
    
    return start_location 
    

def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30,60)) # adds a timedelta that is random between 30 & 60 seconds to the start time
    return start_time
    

def generate_vehicle_data(device_id):
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'device_id': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10, 40),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'X5',
        'year': 2024,
        'fuel_type': 'Hybrid'
        
    }



def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'], 'Camera-123')
        break
        
        
        
        
        

if __name__ == '__main__':
    producer_config= {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'kafka error: {err}')
    }
    producer = SerializingProducer(producer_config)
    
    try:
        simulate_journey(producer, 'Vehicle-Richdaddy-123') #need to create simulate_journey next wi
     
    
    except KeyboardInterrupt:
        print('Simulation ended by the user')
    
    except Exception as e:
        print(f'Unexpected Error occurred: {e}')


