package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// ErrInternalTopic is returned for write operations on system topics.
var ErrInternalTopic = fmt.Errorf("%w: internal topics cannot be modified from kafpeek", ErrInvalid)

// writableTopic checks read-only mode, that the topic exists, and that it is
// not a system topic. It returns the topic's metadata.
func (c *Client) writableTopic(ctx context.Context, name string) (kadm.TopicDetail, error) {
	if c.readOnly {
		return kadm.TopicDetail{}, ErrReadOnly
	}
	topics, err := c.adm.ListTopicsWithInternal(ctx, name)
	if err != nil {
		return kadm.TopicDetail{}, err
	}
	t, ok := topics[name]
	if !ok || errors.Is(t.Err, kerr.UnknownTopicOrPartition) {
		return kadm.TopicDetail{}, fmt.Errorf("topic %q: %w", name, ErrNotFound)
	}
	if t.Err != nil {
		return kadm.TopicDetail{}, t.Err
	}
	if IsInternalTopic(t.Topic, t.IsInternal) {
		return kadm.TopicDetail{}, ErrInternalTopic
	}
	return t, nil
}

// DeleteTopic deletes a topic and all of its data.
func (c *Client) DeleteTopic(ctx context.Context, name string) error {
	if _, err := c.writableTopic(ctx, name); err != nil {
		return err
	}
	resp, err := c.adm.DeleteTopic(ctx, name)
	if err == nil {
		err = resp.Err
	}
	if err != nil {
		return adminErr("deleting", topicSubject(name), err, resp.ErrMessage)
	}
	return nil
}

// SetPartitions grows a topic to count partitions (Kafka cannot shrink).
func (c *Client) SetPartitions(ctx context.Context, name string, count int) error {
	t, err := c.writableTopic(ctx, name)
	if err != nil {
		return err
	}
	if cur := len(t.Partitions); count <= cur {
		return fmt.Errorf("%w: topic already has %d partitions; the count can only grow", ErrInvalid, cur)
	}
	if count > MaxCreatePartitions {
		return fmt.Errorf("%w: at most %d partitions", ErrInvalid, MaxCreatePartitions)
	}
	resps, err := c.adm.UpdatePartitions(ctx, count, name)
	if err != nil {
		return adminErr("adding partitions to", topicSubject(name), err, "")
	}
	resp, err := resps.On(name, nil)
	if err == nil {
		err = resp.Err
	}
	if err != nil {
		return adminErr("adding partitions to", topicSubject(name), err, resp.ErrMessage)
	}
	return nil
}

// TopicConfigChange is an incremental config update: Set overrides values,
// Reset removes overrides (falling back to the broker/cluster default).
type TopicConfigChange struct {
	Set   map[string]string `json:"set"`
	Reset []string          `json:"reset"`
}

// AlterTopicConfigs applies an incremental config change to a topic.
func (c *Client) AlterTopicConfigs(ctx context.Context, name string, ch TopicConfigChange) error {
	if len(ch.Set) == 0 && len(ch.Reset) == 0 {
		return fmt.Errorf("%w: nothing to change", ErrInvalid)
	}
	if len(ch.Set)+len(ch.Reset) > MaxCreateConfigs {
		return fmt.Errorf("%w: at most %d config changes at once", ErrInvalid, MaxCreateConfigs)
	}
	var alters []kadm.AlterConfig
	for k, v := range ch.Set {
		k = strings.TrimSpace(k)
		if k == "" {
			return fmt.Errorf("%w: config names cannot be empty", ErrInvalid)
		}
		alters = append(alters, kadm.AlterConfig{Op: kadm.SetConfig, Name: k, Value: &v})
	}
	for _, k := range ch.Reset {
		k = strings.TrimSpace(k)
		if k == "" {
			return fmt.Errorf("%w: config names cannot be empty", ErrInvalid)
		}
		if _, dup := ch.Set[k]; dup {
			return fmt.Errorf("%w: %q is both set and reset", ErrInvalid, k)
		}
		alters = append(alters, kadm.AlterConfig{Op: kadm.DeleteConfig, Name: k})
	}
	if _, err := c.writableTopic(ctx, name); err != nil {
		return err
	}
	resps, err := c.adm.AlterTopicConfigs(ctx, alters, name)
	if err != nil {
		return adminErr("altering configs of", topicSubject(name), err, "")
	}
	resp, err := resps.On(name, nil)
	if err == nil {
		err = resp.Err
	}
	if err != nil {
		return adminErr("altering configs of", topicSubject(name), err, resp.ErrMessage)
	}
	return nil
}

type PurgeResult struct {
	Partitions int   `json:"partitions"`
	Deleted    int64 `json:"deleted"` // approximate, like TopicSummary.Messages
}

// PurgeTopic deletes every record currently in the given partitions (all
// partitions when empty) by moving their start offset to the end offset.
// Kafka refuses this on compact-only topics (cleanup.policy=compact).
func (c *Client) PurgeTopic(ctx context.Context, name string, partitions []int32) (PurgeResult, error) {
	t, err := c.writableTopic(ctx, name)
	if err != nil {
		return PurgeResult{}, err
	}
	for _, p := range partitions {
		if _, ok := t.Partitions[p]; !ok {
			return PurgeResult{}, fmt.Errorf("%w: partition %d does not exist (topic has %d)", ErrInvalid, p, len(t.Partitions))
		}
	}
	start, end, err := c.offsets(ctx, name)
	if err != nil {
		return PurgeResult{}, err
	}
	var res PurgeResult
	os := make(kadm.Offsets)
	end.Each(func(o kadm.ListedOffset) {
		if o.Err != nil || (len(partitions) > 0 && !slices.Contains(partitions, o.Partition)) {
			return
		}
		res.Partitions++
		if s, ok := start.Lookup(name, o.Partition); ok && s.Err == nil && o.Offset > s.Offset {
			res.Deleted += o.Offset - s.Offset
			os.Add(kadm.Offset{Topic: name, Partition: o.Partition, At: o.Offset, LeaderEpoch: -1})
		}
	})
	if len(os) == 0 {
		return res, nil // already empty
	}
	resps, err := c.adm.DeleteRecords(ctx, os)
	if err != nil {
		return PurgeResult{}, adminErr("purging", topicSubject(name), err, "")
	}
	var errs []error
	resps.Each(func(r kadm.DeleteRecordsResponse) {
		if r.Err != nil {
			errs = append(errs, r.Err)
		}
	})
	if len(errs) > 0 {
		return PurgeResult{}, adminErr("purging", topicSubject(name), errs[0], "")
	}
	return res, nil
}
