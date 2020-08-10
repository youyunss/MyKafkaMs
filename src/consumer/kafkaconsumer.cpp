#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>
#include <cstring>
#include <queue>
#include <vector>
#include <algorithm>
 
#include <sys/time.h>
#include <getopt.h>
#include <unistd.h>
 
#include "rdkafkacpp.h"
#include "../lib/cos_offline_tread_pool.h"
 
static bool run = true;
static bool exit_eof = true;
static int eof_cnt = 0;
static int partition_cnt = 0;
static long msg_cnt = 0;
static int64_t msg_bytes = 0;
 
static void sigterm (int sig) {
    run = false;
}
 
std::string msg_consume(RdKafka::Message* message, void* opaque, std::string& message_content) {
    message_content.clear();
    std::string message_key;
    switch (message->err()) {
    case RdKafka::ERR__TIMED_OUT:
        std::cerr << "RdKafka::ERR__TIMED_OUT"<<std::endl;
        break;

    case RdKafka::ERR_NO_ERROR:
        /* Real message */
        msg_cnt++;
        msg_bytes += message->len();
        message_key.assign(*(message->key()));
        message_content.assign(static_cast<const char *>(message->payload()), static_cast<int>(message->len()));
        /*printf("%.*s, key %s, partition %d, topic %s\n",
                static_cast<int>(message->len()),
                static_cast<const char *>(message->payload()), (*(message->key())).c_str(), message->partition(), message->topic_name().c_str());*/
        break;
 
    case RdKafka::ERR__PARTITION_EOF:
        /* Last message */
        if (exit_eof && ++eof_cnt == partition_cnt) {
            std::cerr << "%% EOF reached for all " << partition_cnt <<" partition(s)" << std::endl;
            run = false;
        }
        break;
 
    case RdKafka::ERR__UNKNOWN_TOPIC:
        std::cerr << "Consume failed: " << message->errstr() << std::endl;
        run = false;
        break;

    case RdKafka::ERR__UNKNOWN_PARTITION:
        std::cerr << "Consume failed: " << message->errstr() << std::endl;
        run = false;
        break;
    
    default:
        /* Errors */
        std::cerr << "Consume failed: " << message->errstr() << std::endl;
        run = false;
    }
    return message_key;
}

class ExampleEventCb : public RdKafka::EventCb {
public:
    void event_cb (RdKafka::Event &event) {
        switch (event.type())
        {
        case RdKafka::Event::EVENT_ERROR:
            std::cerr << "ERROR (" << RdKafka::err2str(event.err()) << "): " <<
                         event.str() << std::endl;
            if (event.err() == RdKafka::ERR__ALL_BROKERS_DOWN)
                run = false;
            break;
 
        case RdKafka::Event::EVENT_STATS:
            std::cerr << "\"STATS\": " << event.str() << std::endl;
            break;
 
        case RdKafka::Event::EVENT_LOG:
            fprintf(stderr, "LOG-%i-%s: %s\n",
                    event.severity(), event.fac().c_str(), event.str().c_str());
            break;
 
        case RdKafka::Event::EVENT_THROTTLE:
            std::cerr << "THROTTLED: " << event.throttle_time() << "ms by " <<
                         event.broker_name() << " id " << (int)event.broker_id() << std::endl;
            break;
 
        default:
            std::cerr << "EVENT " << event.type() <<
                         " (" << RdKafka::err2str(event.err()) << "): " <<
                         event.str() << std::endl;
            break;
        }
    }
};
 
class ExampleConsumeCb : public RdKafka::ConsumeCb {
public:
    void consume_cb (RdKafka::Message &msg, void *opaque) {
        std::string none;
        msg_consume(&msg, opaque, none);
    }
};

class MessageData {
public:
    int job_number_;
    int working_job_id_;
    std::string message_key_;
    std::string message_content_;
    std::vector<int> message_working_state_;
    std::vector<std::string> message_key_vector_;
    std::queue<std::string> message_content_queue_;
    std::vector<std::queue<std::string> > message_content_queue_vector_;
    MessageData() {
        job_number_ = 0;
        working_job_id_ = 0;
    }
};

class MyKafkaConsumer {
public:
    void Init(const char* broker = "localhost:9092", const char* topic = "newtopic", const char* partition = "0", const char* group = "007");
    void Consume_Message(void* p);
    void do_work(void* p);
private:
    std::string broker_;
    std::string topic_;
    std::string group_id_;
    int partition_id_;
    std::string errstr_;
    RdKafka::ErrorCode err_;
    RdKafka::Conf *conf_;
    RdKafka::Conf *tconf_;
    RdKafka::TopicPartition *partition_;
    RdKafka::KafkaConsumer *consumer_;
    ExampleConsumeCb *ex_consume_cb_;
    ExampleEventCb *ex_event_cb_;
    std::vector<std::string> topics_;
    std::vector<RdKafka::TopicPartition*> partitions_;
};

void MyKafkaConsumer::Init(const char* broker, const char* topic, const char* partition, const char* group) {
    broker_ = broker;
    topic_ = topic;
    partition_id_ = atoi(partition);
    group_id_ = group;
    conf_ = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    tconf_ = RdKafka::Conf::create(RdKafka::Conf::CONF_TOPIC);
    //group.id必须设置
    if (conf_->set("group.id", group_id_, errstr_) != RdKafka::Conf::CONF_OK) {
        std::cerr << errstr_ << std::endl;
        exit(1);
    }
    //配置broker
    if (conf_->set("bootstrap.servers", broker_, errstr_) != RdKafka::Conf::CONF_OK) {
        std::cerr << errstr_ << std::endl;
        exit(1);
    }
    conf_->set("consume_cb", ex_consume_cb_, errstr_);
    conf_->set("event_cb", ex_event_cb_, errstr_);
    conf_->set("default_topic_conf", tconf_, errstr_);
    consumer_ = RdKafka::KafkaConsumer::create(conf_, errstr_);
    if (!consumer_) {
        std::cerr << "Failed to create consumer: " << errstr_ << std::endl;
        exit(1);
    }
    std::cout <<"%% Created consumer "<<consumer_->name()<< std::endl;

    partition_ = RdKafka::TopicPartition::create(topic_, partition_id_);
    partitions_.push_back(partition_);
    consumer_->assign(partitions_);

    topics_.push_back(topic_);    
    err_ = consumer_->subscribe(topics_);
    if (err_) {
        std::cerr<<"Failed to subscribe to "<<topics_.size()<<" topics: "<<RdKafka::err2str(err_)<<std::endl;
        exit(1);
    }
}

void MyKafkaConsumer::Consume_Message(void *p) {
    MessageData *message_data_ = (MessageData*)p;
    while (run) {
        //10000毫秒未订阅到消息，触发RdKafka::ERR__TIMED_OUT
        RdKafka::Message *msg = consumer_->consume(10000);
        message_data_->message_key_.assign(msg_consume(msg, NULL, message_data_->message_content_));
        if (!message_data_->message_key_.empty())
        {
            std::vector<std::string>::iterator it = std::find(message_data_->message_key_vector_.begin(), message_data_->message_key_vector_.end(), message_data_->message_key_);
            if (it != message_data_->message_key_vector_.end()) {
                std::cout<<"#The message_key("<<message_data_->message_key_<<") exists, attach the message_content to the queue."<<std::endl;
                message_data_->message_content_queue_vector_[it-message_data_->message_key_vector_.begin()].push(message_data_->message_content_);
            }
            else {
                std::cout<<"#Create new message_content_queue, attach the message_content to new queue. "<<"#A new producer appears!"<<std::endl;
                message_data_->message_working_state_.push_back(0);
                message_data_->message_key_vector_.push_back(message_data_->message_key_);
                message_data_->message_content_queue_.push(message_data_->message_content_);
                message_data_->message_content_queue_vector_.push_back(message_data_->message_content_queue_);
                message_data_->message_content_queue_.pop();
                message_data_->job_number_++;
            }
        }
        delete msg;
    }
    consumer_->close();
    delete conf_;
    delete tconf_;
    delete consumer_;
    std::cerr << "%% Consumed " << msg_cnt << " messages (" << msg_bytes << " bytes)" << std::endl;
    //应用退出之前等待rdkafka清理资源
    RdKafka::wait_destroyed(5000);
}

void MyKafkaConsumer::do_work(void* p) {
    MessageData *message_data_ = (MessageData*)p;
    int queue_number = message_data_->working_job_id_;
    while (run && message_data_->message_content_queue_vector_[queue_number].size() > 0) {
        std::cout<<"##EXECUTE MESSAGE##\t"<<message_data_->message_key_vector_[queue_number]<<"\t"<<message_data_->message_content_queue_vector_[queue_number].front()<<std::endl;
        message_data_->message_content_queue_vector_[queue_number].pop();
    }
    message_data_->message_working_state_[queue_number] = 0;
}

int main (int argc, char **argv) {
    MyKafkaConsumer *kafka_consumer = new MyKafkaConsumer;
    MessageData *message_data = new MessageData;
    std::string flag;
    if(argc != 5){
		std::cout<<"%% Usage: "<<argv[0]<<" <broker> <topic> <partition> <group>"<<std::endl;
        std::cout<<"Do you want to use default_config(localhost:9092 newtopic 0 007) to run this consumer?(yes or no)"<<std::endl;
        std::cin>>flag;
        if (strcmp(flag.c_str(), "yes")) return 1;
        else kafka_consumer->Init();
	} else {
        kafka_consumer->Init(argv[1], argv[2], argv[3], argv[4]);
    }

    signal(SIGINT, sigterm);
    signal(SIGTERM, sigterm);

    Storage::ThreadPool<3> pool;
    pool.AddJob([&]{kafka_consumer->Consume_Message((void*)message_data);});

    int i;
    while (run) {
        i = 0;
        while (run && i < message_data->job_number_) {
            if (message_data->message_content_queue_vector_[i].size() > 0 && message_data->message_working_state_[i] == 0) {
                message_data->message_working_state_[i] = 1;
                message_data->working_job_id_ = i;
                pool.AddJob([&]{kafka_consumer->do_work((void *)message_data);});
            }
            i++;
        }
    }
    return 0;
}