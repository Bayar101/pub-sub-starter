package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	"github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")
	connectionString := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp091.Dial(connectionString)
	if err != nil {
		fmt.Printf("Failed to connect to RabbitMQ: %v", err)
		os.Exit(1)
	}
	defer connection.Close()

	channel, err := connection.Channel()
	if err != nil {
		fmt.Printf("Failed to open a channel: %v", err)
		os.Exit(1)
	}
	defer channel.Close()

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		fmt.Printf("Failed to get username: %v", err)
		os.Exit(1)
	}

	if username == "" {
		fmt.Println("No username provided. goodbye")
		os.Exit(1)
	}

	// Connect to the pause channel
	queueName := fmt.Sprintf("%s.%s", routing.PauseKey, username)
	gameState := gamelogic.NewGameState(username)
	pubsub.SubscribeJSON(connection, routing.ExchangePerilDirect, queueName, routing.PauseKey, pubsub.SimpleQueueTypeTransient, handlerPause(gameState))


	// Connect to the other player's moves
	movesKey := fmt.Sprintf("%s.*", routing.ArmyMovesPrefix)
	movesQueryName := routing.ArmyMovesPrefix + "." + username
	pubsub.SubscribeJSON(connection, routing.ExchangePerilTopic, movesQueryName, movesKey, pubsub.SimpleQueueTypeTransient, handlePlayerMoves(gameState, channel))

	// Connect to the war channel 
	// warQueryName := "war"
	warKey := fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix)
	pubsub.SubscribeJSON(connection, routing.ExchangePerilTopic, routing.WarRecognitionsPrefix, warKey, pubsub.SimpleQueueTypeDurable, handleConsumeWarMessages(gameState, channel))

	for {
		words := gamelogic.GetInput()
		if len(words) == 0{
			continue
		}
		switch words[0] {
		case "spawn": 
			err := gameState.CommandSpawn(words)
			if err != nil {
				fmt.Printf("Failed to spawn units: %v", err)
				continue
			}
		case "move":
			move, err := gameState.CommandMove(words)
			if err != nil {
				fmt.Printf("Failed to move units: %v", err)
				continue
			}
			moveKey := routing.ArmyMovesPrefix + "." + username
			err = pubsub.PublishJSON(channel, routing.ExchangePerilTopic, moveKey, move)
			if err != nil {
				fmt.Printf("Failed to publish move: %v", err)
				continue
			}
			fmt.Printf("Published move to %s\n", moveKey)
		case "status": 
			gameState.CommandStatus()
		case "help":
			gamelogic.PrintClientHelp()
		case "spam":
			if len(words) < 2 {
				fmt.Println("Usage: spam <n>")
				continue
			}
			n, err := strconv.Atoi(words[1])
			if err != nil {
				fmt.Println("Usage: spam <n>")
				continue
			}
			for i := 0; i < n; i++ {
				logMessage := gamelogic.GetMaliciousLog()
				fmt.Println(logMessage)
				err =publishGameLog(channel, routing.GameLog{
					Username: username,
					Message: logMessage,
					CurrentTime: time.Now(),
				})
				if err != nil {
					fmt.Printf("Failed to publish game log: %v", err)
					continue
				}
				// fmt.Println("Spamming not allowed yet!")
			}
			// fmt.Println("Spamming not allowed yet!")
		case "quit":
			gamelogic.PrintQuit()
			os.Exit(1)
		default:
			fmt.Println("Unknown command. Please try again.")
			continue
		}
	}
}


func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlePlayerMoves(gs *gamelogic.GameState, channel *amqp091.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(am gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		outcome :=gs.HandleMove(am)
		switch outcome {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			warKey := fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.GetUsername())
			err := pubsub.PublishJSON(channel, routing.ExchangePerilTopic, warKey, gamelogic.RecognitionOfWar{
				Attacker: am.Player,
				Defender: gs.GetPlayerSnap(),
			})
			if err != nil {
				fmt.Printf("Failed to publish war: %v", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			return pubsub.NackDiscard
		}
	}
}

func handleConsumeWarMessages(gs *gamelogic.GameState, channel *amqp091.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(rw gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		outcome, winner, loser := gs.HandleWar(rw)
		

		switch outcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue
		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard
		case gamelogic.WarOutcomeOpponentWon, gamelogic.WarOutcomeYouWon, gamelogic.WarOutcomeDraw:
			var logMessage string
			if outcome == gamelogic.WarOutcomeDraw {
				logMessage = fmt.Sprintf("A war between %s and %s resulted in a draw", winner, loser)
				}else {
					logMessage = fmt.Sprintf("%s won a war against %s", winner, loser)
				}
			log := routing.GameLog{
				Username: gs.GetUsername(),
				Message: logMessage,
				CurrentTime: time.Now(),
			}

			fmt.Println("——————logMessage——————")
			fmt.Println(logMessage)

			err := publishGameLog(channel, log)
			if err != nil {
				fmt.Printf("Failed to publish game log: %v", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		default:
			return pubsub.NackDiscard
		}
	}
}

func publishGameLog(ch *amqp091.Channel, log routing.GameLog) error {
	key := fmt.Sprintf("%s.%s", routing.GameLogSlug, log.Username)
	err := pubsub.PublishGob(ch, routing.ExchangePerilTopic, key, log)
	if err != nil {
		return fmt.Errorf("failed to publish game log: %v", err)
	}
	return nil
}