import pika

connection = pika.BlockingConnection(
    pika.ConnectionParameters(host='localhost'))

channel = connection.channel()

#创建一个队列 hello
channel.queue_declare(queue='hello')

#使用默认交换机，通过 routing_key 指定要发送的队列
channel.basic_publish(exchange='', routing_key='hello', body='Hello World')
print('message has send')

# 确认网络缓冲已经被刷写、消息已经投递到RabbitMQ
connection.close()