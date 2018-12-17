import time
import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host="localhost"))

channel = connection.channel()

# channel.queue_declare(queue='task_queue', durable=True)
print('[*] Waiting for message. To exit press CTRL+C')


def callback(ch, method, properties, body):
    print("[X] Received %r" % body.decode('utf-8'))
    time.sleep(str(body).count('.'))
    print('[x] Done')
    ch.basic_ack(delivery_tag=method.delivery_tag)


channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback, queue='task_queue', no_ack=False)
channel.start_consuming()
