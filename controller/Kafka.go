package controller

import (
	"cache-manager/config"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"github.com/gocql/gocql"
	// "github.com/gin-gonic/gin"
)

var (
	brokers  = []string{"localhost:9092"} // Replace with your Kafka brokers
	topic    = "example-topic"            // Replace with your Kafka topic
	groupID  = "example-group"            // Replace with your Kafka consumer group ID
	messages = make(chan *sarama.ConsumerMessage, 256)
	wg       sync.WaitGroup
)

// ProduceMessage sends a message to the specified Kafka topic
func ProduceMessage(brokers []string, topic string, message []byte) error {
	config := sarama.NewConfig()
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(brokers, config)
	if err != nil {
		return fmt.Errorf("failed to start Kafka producer: %v", err)
	}
	defer producer.Close()

	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(message),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		return fmt.Errorf("failed to send message: %v", err)
	}

	fmt.Printf("Message sent to partition %d at offset %d\n", partition, offset)
	return nil
}

func KafkaHandler(c *gin.Context) {
	// Read the request body
	var jsonData interface{}
	if err := c.ShouldBindJSON(&jsonData); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Failed to read request body"})
		return
	}

	// Convert jsonData to byte slice
	message, err := json.Marshal(jsonData)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to marshal JSON"})
		return
	}
	// Get brokers and topic from environment or configuration
	brokers := []string{"localhost:9092"} // Replace with your Kafka brokers
	topic := "example-topic"              // Replace with your Kafka topic

	// Send the message to Kafka
	err = ProduceMessage(brokers, topic, message)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": fmt.Sprintf("Failed to send message to Kafka: %v", err)})
		return
	}
	// Respond with a success message
	c.JSON(http.StatusOK, gin.H{"message": "Message sent to Kafka successfully"})
}

// ConsumeMessages consumes messages from the Kafka topic
func ConsumeMessages(brokers []string, groupID string, topics []string) {
	config := sarama.NewConfig()
	config.Consumer.Group.Rebalance.Strategy = sarama.BalanceStrategyRoundRobin
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	client, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		log.Fatalf("Error creating consumer group client: %v", err)
	}
	defer client.Close()

	consumer := Consumer{
		ready: make(chan bool),
	}

	ctx := context.Background()
	for {
		if err := client.Consume(ctx, topics, &consumer); err != nil {
			log.Fatalf("Error from consumer: %v", err)
		}
		// Check if context was cancelled, signaling that the consumer should stop
		if ctx.Err() != nil {
			return
		}
		consumer.ready = make(chan bool)
	}
}

// Consumer represents a Sarama consumer group consumer
type Consumer struct {
	ready   chan bool
	session *gocql.Session
}

// Setup is run at the beginning of a new session, before ConsumeClaim
func (consumer *Consumer) Setup(sarama.ConsumerGroupSession) error {
	// Mark the consumer as ready
	close(consumer.ready)
	return nil
}

// Cleanup is run at the end of a session, once all ConsumeClaim goroutines have exited
func (consumer *Consumer) Cleanup(sarama.ConsumerGroupSession) error {
	return nil
}

// ConsumeClaim must start a consumer loop of ConsumerGroupClaim's Messages().
func (consumer *Consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		log.Printf("Message claimed: value = %s, timestamp = %v, topic = %s", string(message.Value), message.Timestamp, message.Topic)

		// Save the message to Cassandra
		if err := consumer.saveToCassandra(message); err != nil {
			log.Printf("Failed to save message to Cassandra: %v", err)
		}

		sess.MarkMessage(message, "")
	}
	return nil
}

// saveToCassandra saves a Kafka message to the Cassandra logs table
func (consumer *Consumer) saveToCassandra(message *sarama.ConsumerMessage) error {
	// query := `INSERT INTO users_logs (topic,message,timestamp) VALUES (?, ?, toTimestamp(now()))`
	err := config.SESSION.Query("insert into users_logs (topic,message,timestamp) values (?,?,toTimestamp(now()))", message.Topic, message.Value).Exec()
	if err != nil {
		panic(err.Error())
	}
	log.Printf("message to save into Cassandra: %v", string(message.Value))
	return nil
}

// StartConsumerEndpoint starts the Kafka consumer in a separate goroutine
func StartConsumerEndpoint(c *gin.Context) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		ConsumeMessages(brokers, groupID, []string{topic})
	}()
	c.JSON(http.StatusOK, gin.H{"message": "Kafka consumer started"})
}

// StopConsumerEndpoint stops the Kafka consumer
func StopConsumerEndpoint(c *gin.Context) {
	close(messages)
	wg.Wait()
	c.JSON(http.StatusOK, gin.H{"message": "Kafka consumer stopped"})
}
