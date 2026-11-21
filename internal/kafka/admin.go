package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ErrNotFound is returned when a topic or group does not exist.
var ErrNotFound = errors.New("not found")

func (c *Client) Cluster(ctx context.Context) (ClusterInfo, error) {
	meta, err := c.adm.Metadata(ctx)
	if err != nil {
		return ClusterInfo{}, err
	}
	info := ClusterInfo{
		Name:         c.name,
		ClusterID:    meta.Cluster,
		ControllerID: meta.Controller,
	}
	for _, b := range meta.Brokers {
		br := Broker{ID: b.NodeID, Host: b.Host, Port: b.Port}
		if b.Rack != nil {
			br.Rack = *b.Rack
		}
		info.Brokers = append(info.Brokers, br)
	}
	for _, t := range meta.Topics {
		if !IsInternalTopic(t.Topic, t.IsInternal) {
			info.TopicCount++
		}
	}
	return info, nil
}

// IsInternalTopic reports whether a topic is a system topic. Kafka only flags
// __consumer_offsets and __transaction_state as internal, but by ecosystem
// convention any topic starting with "_" is infrastructure, e.g. MSK's
// __amazon_msk_canary / __amazon_msk_canary_state, Schema Registry's _schemas,
// Confluent's __confluent.* and _confluent-*.
func IsInternalTopic(name string, flagged bool) bool {
	return flagged || strings.HasPrefix(name, "_")
}

func (c *Client) Topics(ctx context.Context) ([]TopicSummary, error) {
	topics, err := c.adm.ListTopicsWithInternal(ctx)
	if err != nil {
		return nil, err
	}
	names := topics.Names()
	if len(names) == 0 {
		return []TopicSummary{}, nil
	}
	start, end, err := c.offsets(ctx, names...)
	if err != nil {
		return nil, err
	}
	out := make([]TopicSummary, 0, len(topics))
	for _, t := range topics.Sorted() {
		out = append(out, summarize(t, start, end))
	}
	return out, nil
}

// TopicExists is a cheap metadata-only check.
func (c *Client) TopicExists(ctx context.Context, name string) error {
	topics, err := c.adm.ListTopicsWithInternal(ctx, name)
	if err != nil {
		return err
	}
	t, ok := topics[name]
	if !ok || errors.Is(t.Err, kerr.UnknownTopicOrPartition) {
		return fmt.Errorf("topic %q: %w", name, ErrNotFound)
	}
	return t.Err
}

func (c *Client) Topic(ctx context.Context, name string) (TopicDetail, error) {
	topics, err := c.adm.ListTopicsWithInternal(ctx, name)
	if err != nil {
		return TopicDetail{}, err
	}
	t, ok := topics[name]
	if !ok || errors.Is(t.Err, kerr.UnknownTopicOrPartition) {
		return TopicDetail{}, fmt.Errorf("topic %q: %w", name, ErrNotFound)
	}
	if t.Err != nil {
		return TopicDetail{}, t.Err
	}
	start, end, err := c.offsets(ctx, name)
	if err != nil {
		return TopicDetail{}, err
	}

	detail := TopicDetail{TopicSummary: summarize(t, start, end)}
	for _, p := range t.Partitions.Sorted() {
		part := Partition{ID: p.Partition, Leader: p.Leader, Replicas: p.Replicas, ISR: p.ISR}
		if o, ok := start.Lookup(name, p.Partition); ok {
			part.StartOffset = o.Offset
		}
		if o, ok := end.Lookup(name, p.Partition); ok {
			part.EndOffset = o.Offset
		}
		detail.PartitionList = append(detail.PartitionList, part)
	}

	// Configs are best-effort: on MSK IAM the principal may lack
	// DescribeTopicDynamicConfiguration permission.
	if rcs, err := c.adm.DescribeTopicConfigs(ctx, name); err == nil {
		if rc, err := rcs.On(name, nil); err == nil && rc.Err == nil {
			for _, cfg := range rc.Configs {
				e := ConfigEntry{
					Name:      cfg.Key,
					Source:    cfg.Source.String(),
					Sensitive: cfg.Sensitive,
					IsDefault: cfg.Source == kmsg.ConfigSourceDefaultConfig,
				}
				if cfg.Value != nil {
					e.Value = *cfg.Value
				}
				detail.Configs = append(detail.Configs, e)
			}
			sort.Slice(detail.Configs, func(i, j int) bool { return detail.Configs[i].Name < detail.Configs[j].Name })
		}
	}
	return detail, nil
}

func (c *Client) ConsumerGroups(ctx context.Context) ([]ConsumerGroup, error) {
	listed, err := c.adm.ListGroups(ctx)
	if err != nil {
		return nil, err
	}
	if len(listed) == 0 {
		return []ConsumerGroup{}, nil
	}
	lags, err := c.adm.Lag(ctx, listed.Groups()...)
	if err != nil {
		return nil, err
	}
	out := make([]ConsumerGroup, 0, len(lags))
	for _, l := range lags.Sorted() {
		out = append(out, toGroup(l, false))
	}
	return out, nil
}

func (c *Client) ConsumerGroup(ctx context.Context, name string) (ConsumerGroup, error) {
	lags, err := c.adm.Lag(ctx, name)
	if err != nil {
		return ConsumerGroup{}, err
	}
	l, ok := lags[name]
	if !ok || l.State == "Dead" {
		return ConsumerGroup{}, fmt.Errorf("group %q: %w", name, ErrNotFound)
	}
	return toGroup(l, true), nil
}

func toGroup(l kadm.DescribedGroupLag, withLag bool) ConsumerGroup {
	g := ConsumerGroup{
		Name:     l.Group,
		State:    l.State,
		Protocol: l.ProtocolType,
		Members:  len(l.Members),
		Topics:   []string{},
	}
	if err := errors.Join(l.DescribeErr, l.FetchErr); err != nil {
		g.Error = err.Error()
	}
	for topic := range l.Lag {
		g.Topics = append(g.Topics, topic)
	}
	sort.Strings(g.Topics)
	for _, ml := range l.Lag.Sorted() {
		if ml.Lag > 0 {
			g.TotalLag += ml.Lag
		}
		if !withLag {
			continue
		}
		gl := GroupLag{
			Topic:     ml.Topic,
			Partition: ml.Partition,
			Committed: ml.Commit.At,
			End:       ml.End.Offset,
			Lag:       ml.Lag,
		}
		if ml.Member != nil {
			gl.MemberID = ml.Member.MemberID
		}
		g.Lag = append(g.Lag, gl)
	}
	return g
}

func (c *Client) offsets(ctx context.Context, topics ...string) (start, end kadm.ListedOffsets, err error) {
	if start, err = c.adm.ListStartOffsets(ctx, topics...); err != nil {
		return nil, nil, fmt.Errorf("listing start offsets: %w", err)
	}
	if end, err = c.adm.ListEndOffsets(ctx, topics...); err != nil {
		return nil, nil, fmt.Errorf("listing end offsets: %w", err)
	}
	return start, end, nil
}

func summarize(t kadm.TopicDetail, start, end kadm.ListedOffsets) TopicSummary {
	s := TopicSummary{
		Name:              t.Topic,
		Internal:          IsInternalTopic(t.Topic, t.IsInternal),
		Partitions:        len(t.Partitions),
		ReplicationFactor: t.Partitions.NumReplicas(),
	}
	for _, p := range t.Partitions {
		if p.Leader < 0 {
			s.OfflinePartitions++
		}
		if len(p.ISR) < len(p.Replicas) {
			s.UnderReplicated++
		}
		so, ok1 := start.Lookup(t.Topic, p.Partition)
		eo, ok2 := end.Lookup(t.Topic, p.Partition)
		if ok1 && ok2 && so.Err == nil && eo.Err == nil && eo.Offset > so.Offset {
			s.Messages += eo.Offset - so.Offset
		}
	}
	return s
}
