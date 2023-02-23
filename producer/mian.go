package main

//实现一个kafka生产者
import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"strconv"
	"sync/atomic"
	"time"
)

func main() {
	//SyncProducer() //同步生成者
	AsyncProducer() //异步生产者
}

/*
SyncProducer
@Desc 同步生产者
*/
func SyncProducer() {
	//new 一个默认的kafka配置
	config := sarama.NewConfig()
	//发送完数据等待ack返回方式
	config.Producer.RequiredAcks = sarama.WaitForAll
	//发送到分区
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	//消息发送成功是否通过Successes管道返回,同步生产者必须是true
	config.Producer.Return.Successes = true
	//消息发送失败是否通过Errors管道返回
	config.Producer.Return.Errors = true
	//未收到ack,重试最大次数
	config.Producer.Retry.Max = 3
	//未收到ack,重试等待时间
	config.Producer.Retry.Backoff = time.Millisecond * 300
	//缓冲区
	//config.Producer.Flush =

	//连接kafka服务器
	//连单机
	//address:=string{"192.168.31.27:9092"}
	//连集群
	address := []string{"192.168.31.27:9092", "192.168.31.27:9093", "192.168.31.27:9094"}
	client, err := sarama.NewClient(address, config)
	if err != nil {
		fmt.Printf("kafka connect failed. err:%s", err.Error())
		return
	}
	defer client.Close()

	//获得一个同步生产者
	//同步生产者,发送消息在没收到ack之前会被阻塞
	syncProducer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		fmt.Printf("NewSyncProducerFromClient failed. err:%s", err.Error())
		return
	}

	// 构造一个消息
	msg := &sarama.ProducerMessage{}
	msg.Topic = "test"
	//msg.Value = sarama.StringEncoder("this is a test log") //发字符串
	msg.Value = sarama.ByteEncoder([]byte{67, 67, 67}) //发字节

	// 发送消息
	pid, offset, err := syncProducer.SendMessage(msg)
	if err != nil {
		fmt.Printf("syncProducer SendMessage failed. err:%s", err.Error())
		return
	}
	fmt.Printf("SendMessage success. pid:%v offset:%v\n", pid, offset)
}

/*
AsyncProducer
@Desc 异步生产者
*/
func AsyncProducer() {
	// new 一个默认的kafka配置
	config := sarama.NewConfig()
	// 发送完数据需要leader和follow都确认
	config.Producer.RequiredAcks = sarama.WaitForAll
	// 新选出一个partition
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	//消息发送成功resp是否通过success管道返回
	//如果打开了Return.Successes配置，而又没有producer.Successes()提取，那么Successes()这个chan消息会被写满。
	config.Producer.Return.Successes = true

	//连接kafka服务器
	//如果不想走下面的sarama.NewClient(addrs, config)
	//也可以直接调用sarama.NewSyncProducer(address, config)
	//addrs:=string{"192.168.31.27:9092"}//连单机
	addrs := []string{"192.168.31.27:9092", "192.168.31.27:9093", "192.168.31.27:9094"} //连集群
	client, err := sarama.NewClient(addrs, config)
	if err != nil {
		fmt.Printf("kafka connect failed. err:%s", err.Error())
		return
	}
	defer client.Close()

	//异步生产者
	asyncProducer, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		fmt.Printf("NewSyncProducerFromClient failed. err:%s", err.Error())
		return
	}

	go func() {
		// [!important] 异步生产者发送后必须把返回值从 Errors 或者 Successes 中读出来 不然会阻塞 sarama 内部处理逻辑 导致只能发出去一条消息
		for {
			select {
			case s := <-asyncProducer.Successes():
				log.Printf("[Producer] key:%v msg:%+v \n", s.Key, s.Value)
				break
			case e := <-asyncProducer.Errors():
				if e != nil {
					log.Printf("[Producer] err:%v msg:%+v \n", e.Msg, e.Err)
				}
			}
		}
	}()

	// 异步发送
	var count int64
	for i := 0; i < 10; i++ {
		str := strconv.Itoa(int(time.Now().UnixNano()))
		msg := &sarama.ProducerMessage{Topic: "test", Key: nil, Value: sarama.StringEncoder(str)}
		// 异步发送只是写入内存了就返回了，并没有真正发送出去
		// sarama 库中用的是一个 channel 来接收，后台 goroutine 异步从该 channel 中取出消息并真正发送
		asyncProducer.Input() <- msg
		atomic.AddInt64(&count, 1)
		time.Sleep(time.Second * 1)
		if atomic.LoadInt64(&count)%1000 == 0 {
			log.Printf("已发送消息数:%v\n", count)
		}
	}
	//time.Sleep(time.Second * 1000)
}
