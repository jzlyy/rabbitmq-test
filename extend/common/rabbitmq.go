package common

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"log"
)

const (
	QueueName     = "distributed_task_queue"
	DLXExchange   = "dlx_exchange"
	DLXQueue      = "dead_letter_queue"
	DelayExchange = "delayed_exchange"
)

func ConnectRabbitMQ() (*amqp.Connection, error) {
	conn, err := amqp.Dial("amqp://admin:rabbitmq@172.168.20.101:5672/")
	if err != nil {
		return nil, err
	}
	log.Println("RabbitMQ连接成功")
	return conn, nil
}

func SetupInfrastructure(ch *amqp.Channel) error {
	// 1. 声明死信交换机
	err := ch.ExchangeDeclare(
		DLXExchange,
		"direct",
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,
	)
	if err != nil {
		return err
	}

	// 2. 声明死信队列
	_, err = ch.QueueDeclare(
		DLXQueue,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		nil,
	)
	if err != nil {
		return err
	}

	// 3. 绑定死信队列到交换机
	err = ch.QueueBind(
		DLXQueue,
		"", // routing key
		DLXExchange,
		false, // no-wait
		nil,
	)
	if err != nil {
		return err
	}

	// 4. 声明延迟交换机 (需要安装插件)
	err = ch.ExchangeDeclare(
		DelayExchange,
		"x-delayed-message", // 插件类型
		true,                // durable
		false,               // auto-deleted
		false,               // internal
		false,               // no-wait
		amqp.Table{"x-delayed-type": "direct"},
	)
	if err != nil {
		log.Println("延迟交换机声明失败(请确认已安装插件):", err)
		return err
	}

	// 5. 声明主队列（带优先级和死信设置）
	args := amqp.Table{
		"x-dead-letter-exchange": DLXExchange,
		"x-max-priority":         10, // 优先级0-10
	}
	q, err := ch.QueueDeclare(
		QueueName,
		true,  // durable
		false, // auto-delete
		false, // exclusive
		false, // no-wait
		args,
	)
	if err != nil {
		return err
	}

	// 6. 绑定主队列到延迟交换机
	err = ch.QueueBind(
		q.Name,
		"", // routing key
		DelayExchange,
		false, // no-wait
		nil,
	)
	if err != nil {
		return err
	}

	log.Println("消息队列基础设施初始化完成")
	return nil
}
