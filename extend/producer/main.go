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

	// 4. 发送任务
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	taskID := 1
	rand.Seed(time.Now().UnixNano())

	for {
		select {
		case <-sigCh:
			log.Println("生产者停止")
			return
		default:
			// 随机生成不同类型的任务
			taskType := rand.Intn(3)
			switch taskType {
			case 0: // 普通任务
				task := newTask(taskID, "普通任务")
				publishTask(ch, task, uint8(rand.Intn(5)+1)) // 优先级1-5
				log.Printf(" [x] 发送普通任务: %s (优先级: %d)", task, uint8(rand.Intn(5)+1))

			case 1: // 高优先级任务
				task := newTask(taskID, "高优先级任务")
				publishTask(ch, task, 9) // 高优先级
				log.Printf(" [x] 发送高优先级任务: %s (优先级: 9)", task)

			case 2: // 延迟任务
				delay := time.Duration(rand.Intn(10) + 5) // 5-15秒延迟
				task := newTask(taskID, "延迟任务")
				publishDelayedTask(ch, task, delay*time.Second)
				log.Printf(" [x] 发送延迟任务: %s (延迟: %v秒)", task, delay)
			}

			taskID++
			time.Sleep(2 * time.Second) // 每2秒发送一个任务
		}
	}
}

func newTask(id int, taskType string) string {
	return time.Now().Format("15:04:05") + " - " + taskType + " - ID:" + string(rune(id))
}

// 发送普通/优先级任务
func publishTask(ch *amqp.Channel, body string, priority uint8) {
	err := ch.Publish(
		"",               // exchange
		common.QueueName, // routing key
		false,            // mandatory
		false,            // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(body),
			Priority:     priority,
		})
	failOnError(err, "消息发布失败")
}

// 发送延迟任务
func publishDelayedTask(ch *amqp.Channel, body string, delay time.Duration) {
	headers := amqp.Table{"x-delay": delay.Milliseconds()}

	err := ch.Publish(
		common.DelayExchange, // 发送到延迟交换机
		"",                   // routing key
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(body),
			Headers:      headers,
		})
	failOnError(err, "延迟消息发布失败")
}

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}
