import pika
import sys

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

channel.exchange_declare(exchange='topic_logs', exchange_type='topic')

result = channel.queue_declare(exclusive=True)
queue_name = result.method.queue

binding_keys = sys.argv[1:]

if not binding_keys:
    print(">>> ", sys.stderr, "Usage: binding_key :", sys.argv[0])
    sys.exit(1)

for binding_key in binding_keys:
    channel.queue_bind(
        exchange='topic_logs', queue=queue_name, routing_key=binding_key)

print("[x] : Waiting for logs. To exit process CTRL+C")


def callback(ch, method, properties, body):
    print("[x]", method.routing_key, body.decode('utf-8'))


channel.basic_consume(callback, queue=queue_name, no_ack=True)

channel.start_consuming()
