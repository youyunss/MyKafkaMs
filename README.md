## MyKafkaMS

一个基于kafka中间件的高并发情况下顺序执行消息的消息系统

系统服务器采用kafka官网提供的java脚本来创建，参数kafka/config/server.propertities如下：
#### The id of the broker. This must be set to a unique integer for each broker.
broker.id=0
#### The number of threads that the server uses for receiving requests from the network and sending responses to the network
num.network.threads=3
#### The number of threads that the server uses for processing requests, which may include disk I/O
num.io.threads=8
#### The send buffer (SO_SNDBUF) used by the socket server
socket.send.buffer.bytes=102400
#### The receive buffer (SO_RCVBUF) used by the socket server
socket.receive.buffer.bytes=102400
#### The maximum size of a request that the socket server will accept (protection against OOM)
socket.request.max.bytes=104857600
#### A comma separated list of directories under which to store log files(此处应该调整为相应的路径)
log.dirs=/usr/local/kafka/log
#### The default number of log partitions per topic. More partitions allow greater parallelism for consumption, but this will also result in more files across the brokers.
num.partitions=2
#### The number of threads per data directory to be used for log recovery at startup and flushing at shutdown. This value is recommended to be increased for installations with data dirs located in RAID array.
num.recovery.threads.per.data.dir=1

offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
#### The minimum age of a log file to be eligible for deletion due to age
log.retention.hours=168
#### The maximum size of a log segment file. When this size is reached a new log segment will be created.
log.segment.bytes=1073741824
#### 此处应根据需要设置
zookeeper.connect=localhost:2181
#### Timeout in ms for connecting to zookeeper
zookeeper.connection.timeout.ms=6000
group.initial.rebalance.delay.ms=0
#### 设置为可删除
delete.topic.enable=true



服务器根据生产者使用的key来分配消息到partition，key每个生产者必须赋上不同的key，否则会认为是同一个生产者生产的消息，且容易造成各partition消息分配不均。
消费者则对发布在服务器中的消息进行消费，根据不同的key将对应的消息压入对应的queue中，然后启用程序池开启多线程对不同的队列进行消费，从而保证顺序性。

## 编译

进入项目根目录
```bash
cd build
make

编译完成后，项目文件存放在 `../bin` 目录下

```bash
.
├── bin
│   ├── consumer #消费者
│   └── producer #生产者
├── build
│   └── makefile
├── kafka #kafka服务器脚本
│   ├── bin
│   │   ├── connect-distributed.sh
│   │   ├── connect-standalone.sh
...
├── lib #头文件
│   ├── cos_offline_tread_pool.h
│   ├── kafkaconsumer.h
│   ├── kafkaproducer.h
│   └── librdkafka
│       ├── rdkafkacpp.h
│       ├── rdkafka.h
│       └── rdkafka_mock.h
├── README.md
└── src
    ├── consumer
    │   └── kafkaconsumer.cpp #消费者
    └── producer
        └── kafkaproducer.cpp #生产者
```

## 运行（kafka服务器）
```bash
cd kafka
bin/zookeeper-server-start.sh config/zookeeper.properties 
bin/kafka-server-start.sh config/server.properties 
```

## 运行（服务端）
运行 producer：
```bash
./bin/producer <broker> <topic> <key>
```
必须在kafka脚本启动kafka服务器后运行。

## 运行（服务端）
运行 consumer：
```bash
./bin/consumer <broker> <topic> <partition> <group>
```
上述参数中broker必须是已经建立的broker
