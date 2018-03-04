import json
from os import getenv
from kafka import KafkaConsumer

KAFKA_SESSION_TIMEOUT = 10000

class GdaxStreamCollector(object):

	def __init__(self, kafka_topic=None, kafka_servers=None, kafka_group_id=None):
		self._k_topic = kafka_topic or getenv('CRYPTO_KAFKA_GDAX_TOPIC','gdax')
		self._k_servers = kafka_servers or self.get_kafka_bootstrap_servers()
		self._k_group_id = kafka_group_id or getenv('CRYPTO_GDAX_COLLECTOR_KAFKA_GROUP_ID','gdax')
		self._running = False

	@classmethod
	def get_kafka_bootstrap_servers(cls):
		servers_str = getenv('CRYPTO_KAFKA_BOOTSTRAP_SERVERS', None)
		if servers_str is None: return None
		return list(map(lambda s: s.strip(), servers_str.split(",")))


	def start(self):
		self._running = True
		self._consumer = KafkaConsumer(
			self._k_topic,
			bootstrap_servers=self._k_servers,
			group_id=self._k_group_id,
			enable_auto_commit=False,
		    auto_offset_reset='earliest',
		    session_timeout_ms=KAFKA_SESSION_TIMEOUT,
		    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
		)


