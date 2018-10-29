import pika
import sys

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

channel.exchange_declare(exchange='logs', exchange_type='fanout')

message = '  '.join(sys.argv[1:]) or 'info : hello world!'

channel.basic_publish(exchange='logs', routing_key='black', body=message)

print("[x] Sent %r" % message)