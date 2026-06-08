package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"fmt"

	"github.com/rabbitmq/amqp091-go"
)

type AckType string

const (
	Ack AckType = "Ack"
	NackRequeue AckType = "NackRequeue"
	NackDiscard AckType = "NackDiscard"
)

func PublishJSON[T any](ch *amqp091.Channel, exchange, key string, val T) error {
	json, err := json.Marshal(val)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %v", err)
	}

	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp091.Publishing{
		ContentType: "application/json",	
		Body: json,
	})

	if err != nil {
		return fmt.Errorf("failed to publish message: %v", err)
	}
	return nil
}

type SimpleQueueType string

const (
	SimpleQueueTypeDurable SimpleQueueType = "durable"
	SimpleQueueTypeTransient SimpleQueueType = "transient"
)

func DeclareAndBind(conn *amqp091.Connection, exchange, queueName, key string, queueType SimpleQueueType,) (*amqp091.Channel, amqp091.Queue, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, amqp091.Queue{}, fmt.Errorf("failed to open a channel: %v", err)
	}

	dle := amqp091.Table{
		"x-dead-letter-exchange": "peril_dlx",
	}
	q, err := ch.QueueDeclare(queueName, queueType == SimpleQueueTypeDurable, queueType == SimpleQueueTypeTransient, queueType == SimpleQueueTypeTransient, false, dle)
	if err != nil {
		return nil, amqp091.Queue{}, fmt.Errorf("failed to declare a queue: %v", err)
	}

	err = ch.QueueBind(queueName, key, exchange, false, nil)

	if err != nil {
		return nil, amqp091.Queue{}, fmt.Errorf("failed to bind a queue: %v", err)
	}

	return ch, q, nil
}

func SubscribeJSON[T any](conn *amqp091.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType) error {
	ch, q, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("failed to declare and bind a queue: %v", err)
	}

	err = ch.Qos(10, 0, false)
	if err != nil {
		return fmt.Errorf("failed to set QOS: %v", err)
	}

	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to consume messages: %v", err)
	}

	go func() {
		for msg := range msgs {
			var val T
			err := json.Unmarshal(msg.Body, &val)
			if err != nil {
				fmt.Printf("failed to unmarshal message: %v", err)
				continue
			}
			ackType := handler(val)
			fmt.Printf("received message: %v, ack type: %s\n", val, ackType)
			switch ackType {
			case Ack:
				msg.Ack(false)
			case NackRequeue:
				msg.Nack(false, true)
			default:
				msg.Nack(false, false)
			}
		}
	}()

	return nil
}

func PublishGob[T any](ch *amqp091.Channel, exchange, key string, val T) error {
	var gobVal bytes.Buffer
	encoder := gob.NewEncoder(&gobVal)
	err := encoder.Encode(val)
	if err != nil {
		return fmt.Errorf("failed to encode value: %v", err)
	}
	err = ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp091.Publishing{
		ContentType: "application/gob",
		Body: gobVal.Bytes(),
	})
	
	if err != nil {
		return fmt.Errorf("failed to publish message: %v", err)
	}
	return nil
}



func SubscribeGob[T any](conn *amqp091.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType) error {
	ch, q, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return fmt.Errorf("failed to declare and bind a queue: %v", err)
	}

	err = ch.Qos(10, 0, false)
	if err != nil {
		return fmt.Errorf("failed to set QOS: %v", err)
	}
	msgs, err := ch.Consume(q.Name, "", false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("failed to consume messages: %v", err)
	}

	go func() {
		for msg := range msgs {
			var val T
			decoder := gob.NewDecoder(bytes.NewBuffer(msg.Body))
			err := decoder.Decode(&val)
			if err != nil {
				fmt.Printf("failed to decode message: %v", err)
				continue
			}
			ackType := handler(val)
			fmt.Printf("received message: %v, ack type: %s\n", val, ackType)
			switch ackType {
			case Ack:
				msg.Ack(false)
			case NackRequeue:
				msg.Nack(false, true)
			default:
				msg.Nack(false, false)
			}
		}
	}()
	
	return nil
}