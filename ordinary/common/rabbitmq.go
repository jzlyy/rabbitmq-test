package common

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

const QueueName = "rabbitmq-test"

func ConnectRabbitMQ() (*amqp.Connection, error) {
	conn, err := amqp.Dial("amqp://admin:rabbitmq@172.168.20.101:5672/")
	if err != nil {
		return nil, err
	}
	log.Println("RabbitMQ连接成功")
	return conn, nil
}

func DeclareQueue(ch *amqp.Channel) (amqp.Queue, error) {
	q, err := ch.QueueDeclare(
		QueueName, // 队列名
		true,      // 持久化
		false,     // 自动删除
		false,     // 排他性
		false,     // 不等待
		nil,       // 参数
	)
	if err != nil {
		return amqp.Queue{}, err
	}
	log.Printf("队列声明成功: %s", q.Name)
	return q, nil
}
