import pika
import sys

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()
channel.exchange_declare(exchange='direct_logs', exchange_type='direct')
result = channel.queue_declare(exclusive=True)

queue_name = result.method.queue

serverities = sys.argv[1:]

if not serverities:
    print('>>>', sys.stderr,
          "Usage: %s [info] [warning] [error]" % sys.argv[0])

for serverty in serverities:
    channel.queue_bind(
        exchange='direct_logs', queue=queue_name, routing_key=serverty)

print("[x] Waiting for logs. To exit press exit CTRL+C")


def callback(ch, method, properties, body):
    print("[x]:", method.routing_key, body.decode('utf-8'))


channel.basic_consume(callback, queue=queue_name, no_ack=True)
channel.start_consuming()