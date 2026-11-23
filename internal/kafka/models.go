package kafka

import "time"

type ClusterInfo struct {
	Name         string   `json:"name"`
	ClusterID    string   `json:"clusterId"`
	ControllerID int32    `json:"controllerId"`
	Brokers      []Broker `json:"brokers"`
	TopicCount   int      `json:"topicCount"`
}

type Broker struct {
	ID   int32  `json:"id"`
	Host string `json:"host"`
	Port int32  `json:"port"`
	Rack string `json:"rack,omitempty"`
}

type TopicSummary struct {
	Name              string `json:"name"`
	Internal          bool   `json:"internal"`
	Partitions        int    `json:"partitions"`
	ReplicationFactor int    `json:"replicationFactor"`
	// Messages is an approximation (sum of end-start offsets); compacted
	// topics and transaction markers make it inexact.
	Messages          int64 `json:"messages"`
	UnderReplicated   int   `json:"underReplicated"`
	OfflinePartitions int   `json:"offlinePartitions"`
}

type TopicDetail struct {
	TopicSummary
	PartitionList []Partition   `json:"partitionList"`
	Configs       []ConfigEntry `json:"configs"`
}

type Partition struct {
	ID          int32   `json:"id"`
	Leader      int32   `json:"leader"`
	Replicas    []int32 `json:"replicas"`
	ISR         []int32 `json:"isr"`
	StartOffset int64   `json:"startOffset"`
	EndOffset   int64   `json:"endOffset"`
}

type ConfigEntry struct {
	Name      string `json:"name"`
	Value     string `json:"value"`
	Source    string `json:"source"`
	Sensitive bool   `json:"sensitive"`
	IsDefault bool   `json:"isDefault"`
}

type ConsumerGroup struct {
	Name     string     `json:"name"`
	State    string     `json:"state"`
	Protocol string     `json:"protocolType"`
	Members  int        `json:"members"`
	Topics   []string   `json:"topics"`
	TotalLag int64      `json:"totalLag"`
	Lag      []GroupLag `json:"lag,omitempty"`
	Error    string     `json:"error,omitempty"`
}

type GroupLag struct {
	Topic     string `json:"topic"`
	Partition int32  `json:"partition"`
	Committed int64  `json:"committed"`
	End       int64  `json:"end"`
	Lag       int64  `json:"lag"`
	MemberID  string `json:"memberId,omitempty"`
}

type Message struct {
	Topic     string    `json:"topic"`
	Partition int32     `json:"partition"`
	Offset    int64     `json:"offset"`
	Timestamp time.Time `json:"timestamp"`
	Key       *string   `json:"key"`
	Value     *string   `json:"value"`
	Headers   []Header  `json:"headers,omitempty"`
	// Encoding is "utf8" or "base64" (for binary payloads).
	KeyEncoding   string `json:"keyEncoding,omitempty"`
	ValueEncoding string `json:"valueEncoding,omitempty"`
	ValueSize     int    `json:"valueSize"`
	Truncated     bool   `json:"truncated,omitempty"`
	// Set when the key/value was written with a Schema Registry serializer.
	// On success Key/Value hold the payload decoded to JSON.
	KeySchema   *SchemaRef `json:"keySchema,omitempty"`
	ValueSchema *SchemaRef `json:"valueSchema,omitempty"`
}

type SchemaRef struct {
	ID    int    `json:"id"`
	Type  string `json:"type,omitempty"` // AVRO, PROTOBUF, JSON
	Error string `json:"error,omitempty"`
}

type Header struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}
