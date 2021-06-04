package consumergroup

import (
	"errors"
	"os"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/samuel/go-zookeeper/zk"
	"github.com/wvanbergen/kazoo-go"
)

// TestStartCloseRace replicates the conditions for a race condition that occurred
// when ConsumerGroup.Close() is called immediately after JoinConsumerGroup()
// All the necessary dependencies are mocked with the minimum possible code so that
// the code is functional and the execution enters the panic condition
func TestStartCloseRace(t *testing.T) {
	os.Setenv("TESTING_FAILURE_INJECTION", "1")

	consumer, err := JoinConsumerGroup(
		"ExampleConsumerGroup",
		[]string{"Topic"},
		[]string{"localhost:2181"},
		nil,
		newMockConsumerGroup)
	if err != nil {
		t.Fatal("Could not start consumer")
	}

	// For the race condition to occur close(cg.stopper) in ConsumerGroup.Close() has to be executed
	// after the select in the beginning of ConsumerGroup.topicListConsumer().
	// Normally it is random but a 1 millisecond sleep guarantees that order of execution.
	time.Sleep(1 * time.Millisecond)

	consumer.Close()
}

func newMockConsumerGroup(name string, topics []string, zookeeper []string, config *Config) (cg *ConsumerGroup, err error) {
	if config == nil {
		config = NewConfig()
	}
	config.ClientID = name

	cg = &ConsumerGroup{
		config:   config,
		consumer: &mockSaramaConsumer{},

		kazoo:      &mockZookeeperTopicReader{},
		group:      &mockConsumerGroupManager{},
		groupName:  name,
		instance:   &mockConsumerGroupInstanceManager{},
		instanceID: "test-instance-id",

		messages: make(chan *sarama.ConsumerMessage, config.ChannelBufferSize),
		errors:   make(chan error, config.ChannelBufferSize),
		stopper:  make(chan struct{}),
	}

	offsetConfig := OffsetManagerConfig{CommitInterval: config.Offsets.CommitInterval}
	cg.offsetManager = NewZookeeperOffsetManager(cg, &offsetConfig)

	return cg, nil
}

type mockConsumerGroupManager struct {
}

func (cgm *mockConsumerGroupManager) CommitOffset(string, int32, int64) error {
	return nil
}

func (cgm *mockConsumerGroupManager) Create() error {
	return nil
}

func (cgm *mockConsumerGroupManager) Exists() (bool, error) {
	return true, nil
}

func (cgm *mockConsumerGroupManager) FetchOffset(string, int32) (int64, error) {
	return 1, nil
}

func (cgm *mockConsumerGroupManager) WatchInstances() (kazoo.ConsumergroupInstanceList, <-chan zk.Event, error) {
	ch := make(chan zk.Event, 1)
	cgil := kazoo.ConsumergroupInstanceList{
		&kazoo.ConsumergroupInstance{ID: "test-instance-id"},
		&kazoo.ConsumergroupInstance{ID: "test-instance-id2"},
	}

	// A delay in WatchInstances() gives enough time for cg.Close() to destroy all resources
	// while they are still used in cg.topicConsumer() and cg.partitionConsumer()
	time.Sleep(5 * time.Millisecond)
	return cgil, ch, nil
}

type mockConsumerGroupInstanceManager struct {
}

func (cgim *mockConsumerGroupInstanceManager) Deregister() error {
	return nil
}

func (cgim *mockConsumerGroupInstanceManager) Register(topics []string) error {
	return nil
}

func (cgim *mockConsumerGroupInstanceManager) Registered() (bool, error) {
	return true, nil
}

func (cgim *mockConsumerGroupInstanceManager) ClaimPartition(topic string, partition int32) error {
	return nil
}

func (cgim *mockConsumerGroupInstanceManager) ReleasePartition(topic string, partition int32) error {
	return nil
}

type mockZookeeperTopicReader struct {
}

func (tr *mockZookeeperTopicReader) Close() error {
	// A delay here gives enough time for the panic to occur.
	// If there is no delay the function that calls Close() is finished
	// which also cleans up all the goroutines involved in the panic condition
	time.Sleep(100 * time.Millisecond)
	return nil
}

func (tr *mockZookeeperTopicReader) RetrievePartitionLeaders(partitions kazoo.PartitionList) (partitionLeaders, error) {
	return partitionLeaders{
		partitionLeader{id: 0, leader: 0, partition: &kazoo.Partition{ID: 0, Replicas: nil}},
	}, nil
}

func (tr *mockZookeeperTopicReader) TopicPartitions(topic string) (kazoo.PartitionList, error) {
	// An error is needed here for cg.topicConsumer() to write to the cg.errors channel
	// which causes a panic if the channel is closed
	return nil, errors.New("test error")
}

type mockSaramaConsumer struct {
}

func (c *mockSaramaConsumer) Topics() ([]string, error) {
	return nil, nil
}

func (c *mockSaramaConsumer) Partitions(topic string) ([]int32, error) {
	return nil, nil
}

func (c *mockSaramaConsumer) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	return nil, nil
}

func (c *mockSaramaConsumer) HighWaterMarks() map[string]map[int32]int64 {
	return nil
}

func (c *mockSaramaConsumer) Close() error {
	return nil
}
