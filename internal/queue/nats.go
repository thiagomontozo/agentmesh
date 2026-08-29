package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/thiagomontozo/agentmesh/internal/observability"
)

const (
	streamName     = "AGENTMESH_RUNS"
	consumerName   = "agentmesh-workers"
	executeSubject = "agentmesh.runs.execute"
	dlqSubject     = "agentmesh.runs.dlq"
)

type NATS struct {
	conn     *nats.Conn
	js       jetstream.JetStream
	consumer jetstream.Consumer
}

func NewNATS(ctx context.Context, url string, ackWait time.Duration) (*NATS, error) {
	conn, err := nats.Connect(url, nats.Name("agentmesh"), nats.Timeout(5*time.Second), nats.RetryOnFailedConnect(true), nats.MaxReconnects(-1))
	if err != nil {
		return nil, fmt.Errorf("connect to NATS: %w", err)
	}
	js, err := jetstream.New(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("create JetStream client: %w", err)
	}
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:        streamName,
		Description: "AgentMesh durable run queue and dead letters",
		Subjects:    []string{executeSubject, dlqSubject},
		Storage:     jetstream.FileStorage,
		Retention:   jetstream.LimitsPolicy,
		MaxAge:      7 * 24 * time.Hour,
		Duplicates:  10 * time.Minute,
	}); err != nil {
		conn.Close()
		return nil, fmt.Errorf("configure JetStream stream: %w", err)
	}
	consumer, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
		Durable:       consumerName,
		Name:          consumerName,
		Description:   "AgentMesh run workers",
		AckPolicy:     jetstream.AckExplicitPolicy,
		AckWait:       ackWait,
		MaxDeliver:    -1,
		FilterSubject: executeSubject,
	})
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("configure JetStream consumer: %w", err)
	}
	return &NATS{conn: conn, js: js, consumer: consumer}, nil
}

func (q *NATS) Enqueue(ctx context.Context, runID string) error {
	_, err := q.js.Publish(ctx, executeSubject, []byte(runID), jetstream.WithMsgID(runID))
	if err != nil {
		return fmt.Errorf("publish run: %w", err)
	}
	return nil
}

func (q *NATS) Consume(ctx context.Context, workers int, handler Handler) error {
	workerSlots := make(chan int, workers)
	for workerID := 1; workerID <= workers; workerID++ {
		workerSlots <- workerID
	}
	var wg sync.WaitGroup
	errCh := make(chan error, 1)
	consumeCtx, err := q.consumer.Consume(func(msg jetstream.Msg) {
		select {
		case workerID := <-workerSlots:
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { workerSlots <- workerID }()
				workerCtx := observability.WithWorkerID(ctx, fmt.Sprintf("nats-%d", workerID))
				if err := handler(workerCtx, string(msg.Data())); err != nil {
					_ = msg.NakWithDelay(time.Second)
					return
				}
				ackCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				_ = msg.DoubleAck(ackCtx)
			}()
		case <-ctx.Done():
			return
		}
	}, jetstream.PullMaxMessages(workers*2), jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
		select {
		case errCh <- err:
		default:
		}
	}))
	if err != nil {
		return fmt.Errorf("consume JetStream runs: %w", err)
	}

	select {
	case <-ctx.Done():
		consumeCtx.Stop()
		<-consumeCtx.Closed()
		wg.Wait()
		return nil
	case err := <-errCh:
		consumeCtx.Stop()
		<-consumeCtx.Closed()
		wg.Wait()
		return fmt.Errorf("JetStream consumer: %w", err)
	}
}

func (q *NATS) DeadLetter(ctx context.Context, runID string, cause error) error {
	payload, err := json.Marshal(struct {
		RunID string    `json:"run_id"`
		Error string    `json:"error"`
		At    time.Time `json:"at"`
	}{RunID: runID, Error: cause.Error(), At: time.Now().UTC()})
	if err != nil {
		return err
	}
	if _, err := q.js.Publish(ctx, dlqSubject, payload, jetstream.WithMsgID("dlq-"+runID)); err != nil {
		return fmt.Errorf("publish dead letter: %w", err)
	}
	return nil
}

func (q *NATS) Ping(ctx context.Context) error {
	if !q.conn.IsConnected() {
		return fmt.Errorf("NATS is not connected")
	}
	return q.conn.FlushWithContext(ctx)
}

func (q *NATS) Close() error {
	if q.conn == nil || q.conn.IsClosed() {
		return nil
	}
	if err := q.conn.Drain(); err != nil {
		q.conn.Close()
		return err
	}
	return nil
}

var _ Queue = (*NATS)(nil)
