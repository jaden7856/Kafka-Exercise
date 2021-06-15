from kafka import KafkaProducer
from json import dumps
import time


# dict (key, value) -> object
# str -> string
producer = KafkaProducer(acks=0, 
            compression_type='gzip',
            bootstrap_servers=['127.0.0.1:9092'],
            value_serializer=lambda x : dumps(x).encode('utf-8'))

start = time.time()
for i in range(10):
    # data = {'name': 'Dowon-' + str(i)}
    {"schema":{"type":"struct","fields":[{"type":"int32","optional":false,"field":"id"},{"type":"string","optional":false,"field":"user_id"},{"type":"string","optional":false,"field":"pwd"},{"type":"int64","optional":true,"name":"org.apache.kafka.connect.data.Timestamp","version":1,"field":"created_at"}],"optional":false,"name":"users"},"payload":{"id":19,"user_id":"JOJOJO","pwd":"test2222","created_at":1617350192000}}

    data = {"schema":{"type":"struct","fields":[{"type":"int32","field":"id"},{"type":"string","field":"user_id"},{"type":"string","field":"pwd"},{"type":"int64","name":"org.apache.kafka.connect.data.Timestamp","version":1,"field":"created_at"}],"name":"users"},"payload":{"id":100,"user_id":"JOJOJO","pwd":"test2222","created_at":1615349727000}}


    producer.send('my_topic_users', value=data)
    producer.flush()

print("Doen. Elapsed time: ", (time.time() - start))