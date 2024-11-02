from confluent_kafka import Producer
import json
import time
import random
from prettytable import PrettyTable

def create_producer():
    conf = {'bootstrap.servers': 'localhost:9092'}
    return Producer(conf)

def simulate_sensor_data():
    sensors = {
        'S1': {'min': 65, 'max': 85, 'location': 'Area-1'},
        'S2': {'min': 70, 'max': 90, 'location': 'Area-2'},
        'S3': {'min': 60, 'max': 82, 'location': 'Area-3'}
    }
    
    producer = create_producer()
    print("Mulai mengirim data sensor...")
    
    # Tabel untuk data yang dikirim
    table = PrettyTable()
    table.field_names = ["Sensor ID", "Lokasi", "Suhu (°C)", "Timestamp"]

    try:
        while True:
            for sensor_id, config in sensors.items():
                data = {
                    'sensor_id': sensor_id,
                    'suhu': round(random.uniform(config['min'], config['max']), 1),
                    'lokasi': config['location'],
                    'timestamp': time.strftime('%Y-%m-%d %H:%M:%S')
                }
                
                msg = json.dumps(data)
                producer.produce('sensor-suhu', value=msg)
                producer.flush()
                
                table.add_row([data['sensor_id'], data['lokasi'], data['suhu'], data['timestamp']])
                
                print("\nData Terkirim:")
                print(table)
                print("\n=========================\n")
                
                time.sleep(1)
                
    except KeyboardInterrupt:
        print("\nProducer dihentikan")
        producer.flush()

if __name__ == "__main__":
    simulate_sensor_data()
