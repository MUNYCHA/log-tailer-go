package kafka

import (
	"log/slog"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

const errLogInterval = 5 * time.Second

func NewProducer(bootstrapServers string) (sarama.AsyncProducer, error) {
	brokers := strings.Split(bootstrapServers, ",")
	for i := range brokers {
		brokers[i] = strings.TrimSpace(brokers[i])
	}

	cfg := sarama.NewConfig()
	// Small internal queues so a Kafka outage backpressures the tailers
	// early instead of buffering thousands of messages in memory
	cfg.ChannelBufferSize = 32
	cfg.Producer.RequiredAcks = sarama.WaitForLocal    // acks=1
	cfg.Producer.Flush.Frequency = 5 * time.Millisecond // linger.ms=5
	cfg.Producer.Flush.Bytes = 65536                    // batch.size=65536
	cfg.Producer.Return.Errors = true
	cfg.Producer.Return.Successes = false

	return sarama.NewAsyncProducer(brokers, cfg)
}

// DrainErrors consumes the producer error channel and logs failures with throttling.
// Blocks until the producer is closed. Must be run in a goroutine.
func DrainErrors(producer sarama.AsyncProducer) {
	var (
		failures int64
		lastLog  time.Time
	)

	for err := range producer.Errors() {
		failures++
		if time.Since(lastLog) >= errLogInterval {
			slog.Error("Kafka delivery failing",
				"topic", err.Msg.Topic,
				"failures_since_last_log", failures,
				"error", err.Err,
			)
			lastLog = time.Now()
			failures = 0
		}
	}
}
