# 备注

## 参考资料

- 文档
  - [入门手册](http://rabbitmq.mr-ping.com/tutorials_with_python/[1]Hello_World.html)
  - [配置文档](http://www.rabbitmq.com/configure.html#config-items)
  - [命令使用](http://www.rabbitmq.com/man/rabbitmqctl.8.html)

## Notice

### Hello World

- 在发送消息之前，我们需要确认服务于消费者的队列已经存在。如果将消息发送给一个不存在的队列，RabbitMQ 会将消息丢弃掉。

- 在 RabbitMQ 中，消息是不能直接发送到队列中的，这个过程需要通过交换机（exchange）来进行。但是为了不让细节拖累我们的进度，这里我们只需要知道如何使用由空字符串表示的默认交换机即可。默认交换机比较特别，它允许我们指定消息究竟需要投递到哪个具体的队列中，队列名字需要在 routing_key 参数中指定。

### 工作队列

> 当我们把任务（Task）当作消息发送到队列中，一个运行在后台的工作者（worker）进程就会取出任务然后处理。当你运行多个工作者（workers），任务就会在它们之间共享。

- 默认情况下，多个 worker 中有一个异常退出后，会发送一个 ack 确认帧给 RabbitMQ, RabbitMQ 会认为这个消息没有被处理，这个消息会被发发送给中这个队列中的其他 work。 可以通过 no_ack 参数关闭此设置：`channel.basic_consume(callback, queue='task_queue', no_ack=True)`

#### 循环调度

RabbitMQ 会把消息均等的分发给队列中的所有 work。

#### 消息持久化

默认情况下 RabbitMQ 会在退出或者崩溃的时候丢失所有队列和消息。 为了确保信息不会丢失，有两个事情是需要注意的：_我们必须把“队列”和“消息”设为持久化_。

- 队列持久化(durable 参数): `channel.queue_declare(queue='task_queue', durable=True)`
- 消息持久化(delivery_mode 参数):

  ```py
    channel.basic_publish(
    exchange='',
    routing_key='task_queue',
    body=message,
    properties=pika.BasicProperties(delivery_mode=2))

  ```

#### 公平调度

多个 work 之间并没有如期待的平均分发到消息。因为此时， RabbitMQ 只管分发进入队列的消息，不会关心有多少消费者（consumer）没有作出响应。它盲目的把第 n-th 条消息发给第 n-th 个消费者。
可以使用 basic.qos 方法，并设置 prefetch_count=1。这样是告诉 RabbitMQ，再同一时刻，不要发送超过 1 条消息给一个工作者（worker），直到它已经处理了上一条消息并且作出了响应。这样，RabbitMQ 就会把消息分发给下一个空闲的工作者（worker）。

```
channel.basic_qos(prefetch_count=1)
```

### 交换机

RabbitMQ 的核心: 发布者不直接把消息发送给队列（甚至不知道消息是否被发送到队列），发布者只把消息发送给交换机。交换机一面接收发布者的消息一面把消息推送给接受者。 交换机必须知道如何处理它接收到的消息，是应该推送到指定的队列还是是多个队列，或者是直接忽略消息。这些规则是通过交换机类型（exchange type）来定义的。

#### 交换机类型

- 直连交换机(direct): 交换机将会对绑定键（binding key）和路由键（routing key）进行精确匹配，从而确定消息该分发到哪个队列。
  - 绑定键: 绑定（binding）是指交换机（exchange）和队列（queue）的关系
  - 路由键：routing_key 参数
- 主题交换机(topic):
- 头叫环节(headers):
- 扇形交换机(fanout): 把消息发送给它所知道的所有队列，会忽略 routing_key 参数值

#### 临时队列

- 随机队列名: `result = channel.queue_declare()`
- 当消费者断开的时候队列被删除: `result = channel.queue_declare(exclusive=True)`，result 是返回的队列名称。

## 命令

- 关闭服务: `rabbitmqctl stop`
- 启动服务: `rabbitmqctl start_app`
- 列出所有队列: `rabbitmqctl list_queues`
- 列出不能识别的消息:`rabbitmqctl list_queues name messages_ready messages_unacknowledged`
- 列出交换机类型: `rabbitmqctl list_exchanges`
- 列出绑定列表: `rabbitmqctl list_bindings`
