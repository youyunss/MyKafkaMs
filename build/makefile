all:consumer producer
.PHONY:all
consumer:
	g++ -g -Wall -o ../bin/consumer ../src/consumer/kafkaconsumer.cpp -I../lib -I../lib/librdkafka/ -L/usr/local/include/ -lpthread -lrdkafka -lrdkafka++ --std=c++11
producer:
	g++ -g -Wall -o ../bin/producer ../src/producer/kafkaproducer.cpp -I../lib -I../lib/librdkafka/ -L/usr/local/include/ -lpthread -lrdkafka -lrdkafka++ --std=c++11
