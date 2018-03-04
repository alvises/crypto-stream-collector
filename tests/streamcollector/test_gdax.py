# filters only trades
# converts size into decimal
# converts price into decimal

# if the table does
# creates the table and hypertable

# write redundant data, multiple times the same trade
# UPSERT using trade_id

# different product ids then different tables gdax_productid

import pytest

from streamcollector.gdax import GdaxStreamCollector

@pytest.fixture
def redundant_mix_trades_and_other():
	return [
	{'type': 'match', 'trade_id': 138275, 'side': 'buy', 'size': '0.02057584', 'price': '0.11843000', 'product_id': 'BCH-BTC', 'sequence': 45719232,'time': '2018-03-01T13:19:57.391000Z'},
	{'type': 'match', 'trade_id': 138275, 'side': 'buy', 'size': '0.02057584', 'price': '0.11843000', 'product_id': 'BCH-BTC', 'sequence': 45719232,'time': '2018-03-01T13:19:57.391000Z'},
	{'type': 'match', 'trade_id': 138275, 'side': 'buy', 'size': '0.02057584','price': '0.11843000', 'product_id': 'BCH-BTC', 'sequence': 45719232,'time': '2018-03-01T13:19:57.391000Z'},
	{'type': 'match', 'trade_id': 138276, 'side': 'buy', 'size': '0.67193741','price': '0.11843000', 'product_id': 'BCH-BTC', 'sequence': 45719242,'time': '2018-03-01T13:19:57.545000Z'},
	{'type': 'match', 'trade_id': 138276, 'side': 'buy', 'size': '0.67193741','price': '0.11843000', 'product_id': 'BCH-BTC', 'sequence': 45719242,'time': '2018-03-01T13:19:57.545000Z'},
	{'type': 'match', 'trade_id': 3784778, 'side': 'buy', 'size': '0.10000000', 'price': '0.02007000','product_id': 'LTC-BTC', 'sequence': 434213951, 'time': '2018-02-28T04:30:44.811000Z'},
	{'type': 'match', 'trade_id': 3784778, 'side': 'buy', 'size': '0.10000000', 'price': '0.02007000','product_id': 'LTC-BTC', 'sequence': 434213951, 'time': '2018-02-28T04:30:44.811000Z'},
	{'type': 'match', 'trade_id': 137372, 'side': 'buy', 'size': '0.05593320', 'price': '0.11621000','product_id': 'BCH-BTC', 'sequence': 44621417, 'time': '2018-02-28T23:05:27.372000Z'},
	{'type': 'match', 'trade_id': 137393, 'side': 'sell', 'size': '0.91349233', 'price': '0.11645000','product_id': 'BCH-BTC', 'sequence': 44639183, 'time': '2018-02-28T23:20:07.919000Z'},
	{'type': 'match', 'trade_id': 137372, 'side': 'buy', 'size': '0.05593320', 'price': '0.11621000','product_id': 'BCH-BTC', 'sequence': 44621417, 'time': '2018-02-28T23:05:27.372000Z'},
	{'type': 'match', 'trade_id': 137393, 'side': 'sell', 'size': '0.91349233', 'price': '0.11645000','product_id': 'BCH-BTC', 'sequence': 44639183, 'time': '2018-02-28T23:20:07.919000Z'}
]

class TestGdaxStreamCollector(object):

	def test__get_kafka_bootstrap_servers__returns_none_if_no_env_var_is_set(self):
		assert GdaxStreamCollector().get_kafka_bootstrap_servers() == None

	def test__get_kafka_bootstrap_servers__returns_array_of_servers(self):
		from os import environ
		environ['CRYPTO_KAFKA_BOOTSTRAP_SERVERS'] = "kafka-0:9092 , kafka-1:9092"
		servers = GdaxStreamCollector.get_kafka_bootstrap_servers()
		assert servers == ["kafka-0:9092","kafka-1:9092"]

	# def test__filtered_trades__gets_only_trades(self):
	# def test__start__connects_to_kafka(self):
