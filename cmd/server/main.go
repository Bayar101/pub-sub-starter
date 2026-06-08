package main

import (
	"fmt"
	"os"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")
	connectionString := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(connectionString)
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ: %v", err)
		os.Exit(1)
	}
	defer connection.Close()

	gamelogic.PrintServerHelp()

	channel, err := connection.Channel()
	if err != nil {
		fmt.Printf("Failed to open a channel: %v", err)
		os.Exit(1)
	}
	defer channel.Close()

	queueName := routing.GameLogSlug
	queueKey := routing.GameLogSlug + ".*"
	pubsub.DeclareAndBind(connection, routing.ExchangePerilDirect, queueName, queueKey, pubsub.SimpleQueueTypeDurable)
	// 
	pubsub.SubscribeGob(connection, routing.ExchangePerilTopic, queueName, queueKey, pubsub.SimpleQueueTypeDurable, handleGameLog())

	for {
		words := gamelogic.GetInput()
		if len(words) == 0 {
			continue
		}

		switch words[0] {
		case "pause":
			fmt.Println("Sending pause message...")
			err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: true,
			})
			if err != nil {
				fmt.Printf("Failed to publish message: %v", err)
				os.Exit(1)
			}
		case "resume":
			fmt.Println("Sending resume message...")
			err = pubsub.PublishJSON(channel, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{
				IsPaused: false,
			})
			if err != nil {
				fmt.Printf("Failed to publish message: %v", err)
				os.Exit(1)
			}
		case "quit":
			fmt.Println("Exiting...")
			goto done
		default:
			fmt.Println("I don't understand that command.")
		}
	}

done:
}


func handleGameLog() func(routing.GameLog) pubsub.AckType {
	return func(log routing.GameLog) pubsub.AckType {
		defer fmt.Print("> ")
		gamelogic.WriteLog(log)
		return pubsub.Ack
	}
}
