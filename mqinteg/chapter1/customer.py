import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

# 消费者使用 queue_declare 创建同一个队列，但是只有一个队列会被创建
channel.queue_declare(queue='hello')


def callback(channel, method, properties, body):
    print('[x] Received %r' % body.decode('utf-8'))


channel.basic_consume(callback, queue='hello', no_ack=True)

print('[*] Waiting for messages. To exit press CTRL+C')

channel.start_consuming()