package main

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"rabbitmq-gz/common"
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

	// 3. 设置消息队列基础设施
	err = common.SetupInfrastructure(ch)
	failOnError(err, "基础设施设置失败")

	// 4. 设置QoS（公平分发）
	err = ch.Qos(
		1,     // prefetch count
		0,     // prefetch size
		false, // global
	)
	failOnError(err, "QoS设置失败")

	// 5. 消费主队列
	msgs, err := ch.Consume(
		common.QueueName,
		"",    // consumer
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	failOnError(err, "注册消费者失败")

	// 6. 消费死信队列
	dlxMsgs, err := ch.Consume(
		common.DLXQueue,
		"dlx_consumer",
		false, // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,
	)
	failOnError(err, "死信队列注册失败")

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	log.Println(" [*] 等待任务. 按 CTRL+C 退出")

	// 处理死信队列的goroutine
	go func() {
		for msg := range dlxMsgs {
			handleDeadLetter(msg)
		}
	}()

	// 主循环处理普通消息
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
	log.Printf(" [x] 收到任务: %s (优先级: %d)", msg.Body, msg.Priority)

	// 模拟任务处理 - 随机失败率20%
	if rand.Intn(5) == 0 {
		log.Printf(" [x] 任务处理失败: %s", msg.Body)
		msg.Nack(false, false) // 不重新入队，转到死信队列
		return
	}

	// 模拟处理时间 (1-5秒)
	processTime := time.Duration(rand.Intn(4) + 1)
	time.Sleep(processTime * time.Second)

	log.Printf(" [✓] 任务完成: %s (耗时: %v秒)", msg.Body, processTime)

	// 手动确认消息
	err := msg.Ack(false)
	if err != nil {
		log.Printf("消息确认失败: %v", err)
	}
}

func handleDeadLetter(msg amqp.Delivery) {
	log.Printf(" [☠️] 收到死信: %s", msg.Body)
	log.Printf("     原因: %v", msg.Headers["x-death"])

	// 记录死信信息到日志/数据库
	// ...

	// 确认死信消息
	err := msg.Ack(false)
	if err != nil {
		log.Printf("死信确认失败: %v", err)
	}
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Printf("%s: %s", msg, err)
	}
}
