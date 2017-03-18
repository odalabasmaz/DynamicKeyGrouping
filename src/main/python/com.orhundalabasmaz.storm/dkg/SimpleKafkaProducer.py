#
# @author Orhun Dalabasmaz
#

from kafka import KafkaProducer

from config.KafkaConfig import kafka_bootstrap_servers


class SimpleKafkaProducer:
    def __init__(self):
        self.producer = KafkaProducer(bootstrap_servers=kafka_bootstrap_servers)

    def send_message(self, topic, msg, key=None):
        # print("# sending msg: ", key, msg)
        self.producer.send(topic, msg, key)
        # self.producer.flush()
