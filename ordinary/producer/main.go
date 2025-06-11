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

	// 3. 声明持久化队列
	_, err = common.DeclareQueue(ch)
	failOnError(err, "队列声明失败")

	// 4. 发送任务
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	taskID := 1
	for {
		select {
		case <-sigCh:
			log.Println("生产者停止")
			return
		default:
			task := newTask(taskID)
			publishTask(ch, task)
			taskID++
			time.Sleep(1 * time.Second) // 每秒发送一个任务
		}
	}
}

func newTask(id int) string {
	return time.Now().Format("2006-01-02 15:04:05") + " 任务ID:" + string(rune(id))
}

func publishTask(ch *amqp.Channel, body string) {
	err := ch.Publish(
		"",               // 使用默认交换机
		common.QueueName, // 路由键（队列名）
		false,            // 强制标志
		false,            // 立即标志
		amqp.Publishing{
			DeliveryMode: amqp.Persistent, // 持久化消息
			ContentType:  "text/plain",
			Body:         []byte(body),
		})
	failOnError(err, "消息发布失败")
	log.Printf(" [x] 已发送: %s", body)
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}
