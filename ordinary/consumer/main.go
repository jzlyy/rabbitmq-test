package main

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"os"
	"os/signal"
	"rabbitmq-test/common"
	"time"
)

func main() {
	// 1. 建立连接
	conn, err := common.ConnectRabbitMQ()
	failOnError(err, "连接失败")
	defer conn.Close()

	// 2. 创建通道
	ch, err := conn.Channel()
	failOnError(err, "打开通道失败")
	defer ch.Close()

	// 3. 声明队列
	q, err := common.DeclareQueue(ch)
	failOnError(err, "队列声明失败")

	// 4. 设置QoS（公平分发）
	err = ch.Qos(
		1,     // 每次预取1条消息
		0,     // 预取大小（0=无限制）
		false, // 全局设置
	)
	failOnError(err, "QoS设置失败")

	// 5. 消费消息
	msgs, err := ch.Consume(
		q.Name, // 队列名
		"",     // 消费者标识（自动生成）
		false,  // 手动确认
		false,  // 非排他
		false,  // 不阻塞
		false,  // 无额外参数
		nil,
	)
	failOnError(err, "注册消费者失败")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	log.Println(" [*] 等待任务. 按 CTRL+C 退出")

	for {
		select {
		case <-sigCh:
			log.Println("消费者停止")
			return
		case msg := <-msgs:
			processTask(msg)
		}
	}
}

func processTask(msg amqp.Delivery) {
	log.Printf(" [x] 收到任务: %s", msg.Body)

	// 模拟任务处理耗时
	time.Sleep(5 * time.Second)

	log.Printf(" [x] 任务完成: %s", msg.Body)

	// 手动确认消息
	err := msg.Ack(false)
	failOnError(err, "消息确认失败")
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Printf("%s: %s", msg, err)
	}
}
