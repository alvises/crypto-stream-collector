# it starts to get data from kafka from the same offset before stopping

# writes into kafka different trades (all trades) and insert them into db

# writes into kafka mix of trades and other data, filters them

# write redundant data, multiple times the same trade
# import pip
# pip.main(['install','kafka-python'])
# import json
# from kafka import KafkaConsumer
# consumer = KafkaConsumer('gdax',
# 	bootstrap_servers=["kafka-0:9092","kafka-1:9092","kafka-2:9092"],
# 	group_id='temp_1',
# 	# enable_auto_commit=True,
# 	auto_offset_reset='earliest',
# 	session_timeout_ms=100000,
# 	value_deserializer=lambda m: json.loads(m.decode('utf-8'))
# )
#
#
# for message in consumer: print(message)