package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
)

// Reset strategies, matching kafka-consumer-groups.sh --reset-offsets.
const (
	ResetEarliest  = "earliest"  // start of each partition
	ResetLatest    = "latest"    // end of each partition (skip everything)
	ResetTimestamp = "timestamp" // first offset at or after Timestamp (ms)
	ResetOffset    = "offset"    // absolute Offset
	ResetShift     = "shift"     // committed offset + Offset (may be negative)
)

type ResetOffsetsRequest struct {
	Topic string `json:"topic"`
	// Partitions to reset; empty means every partition of Topic.
	Partitions []int32 `json:"partitions"`
	To         string  `json:"to"`
	Timestamp  *int64  `json:"timestamp"` // for "timestamp", Unix ms
	Offset     *int64  `json:"offset"`    // for "offset" (absolute) and "shift" (delta)
	// DryRun computes the plan without committing anything.
	DryRun bool `json:"dryRun"`
}

type OffsetChange struct {
	Partition int32 `json:"partition"`
	// Skipped is set for partitions without a commit when shifting across
	// all partitions: there is nothing to shift from, so they are left alone.
	Skipped   bool  `json:"skipped,omitempty"`
	Current   int64 `json:"current"` // -1 when the group has no commit for it
	Target    int64 `json:"target"`
	Start     int64 `json:"start"`
	End       int64 `json:"end"`
	LagBefore int64 `json:"lagBefore"` // -1 when there is no commit
	LagAfter  int64 `json:"lagAfter"`
}

type ResetOffsetsResult struct {
	Group   string `json:"group"`
	Topic   string `json:"topic"`
	State   string `json:"state"`
	Members int    `json:"members"`
	// Active is true when the group has members: Kafka only accepts the
	// reset once every consumer of the group is stopped.
	Active  bool           `json:"active"`
	Applied bool           `json:"applied"`
	Changes []OffsetChange `json:"changes"`
}

func (req ResetOffsetsRequest) validate() error {
	if req.Topic == "" {
		return fmt.Errorf("%w: topic is required", ErrInvalid)
	}
	switch req.To {
	case ResetEarliest, ResetLatest:
	case ResetTimestamp:
		if req.Timestamp == nil || *req.Timestamp < 0 {
			return fmt.Errorf("%w: timestamp (Unix ms) is required", ErrInvalid)
		}
	case ResetOffset:
		if req.Offset == nil || *req.Offset < 0 {
			return fmt.Errorf("%w: a non-negative offset is required", ErrInvalid)
		}
	case ResetShift:
		if req.Offset == nil || *req.Offset == 0 {
			return fmt.Errorf("%w: a non-zero shift is required", ErrInvalid)
		}
	default:
		return fmt.Errorf("%w: \"to\" must be one of earliest, latest, timestamp, offset, shift", ErrInvalid)
	}
	return nil
}

// ResetOffsets moves a consumer group's committed offsets on one topic. With
// DryRun it only returns the plan; otherwise it commits, which Kafka allows
// only while the group has no active members.
func (c *Client) ResetOffsets(ctx context.Context, group string, req ResetOffsetsRequest) (ResetOffsetsResult, error) {
	if c.readOnly && !req.DryRun {
		return ResetOffsetsResult{}, ErrReadOnly
	}
	if err := req.validate(); err != nil {
		return ResetOffsetsResult{}, err
	}
	res := ResetOffsetsResult{Group: group, Topic: req.Topic, Changes: []OffsetChange{}}

	described, err := c.adm.DescribeGroups(ctx, group)
	if err != nil {
		return res, adminErr("describing", groupSubject(group), err, "")
	}
	if g, ok := described[group]; ok {
		if g.Err != nil && !errors.Is(g.Err, kerr.GroupIDNotFound) {
			return res, adminErr("describing", groupSubject(group), g.Err, g.ErrMessage)
		}
		res.State, res.Members = g.State, len(g.Members)
		res.Active = len(g.Members) > 0
	}

	topics, err := c.adm.ListTopicsWithInternal(ctx, req.Topic)
	if err != nil {
		return res, err
	}
	td, ok := topics[req.Topic]
	if !ok || errors.Is(td.Err, kerr.UnknownTopicOrPartition) {
		return res, fmt.Errorf("topic %q: %w", req.Topic, ErrNotFound)
	}
	if td.Err != nil {
		return res, td.Err
	}
	for _, p := range req.Partitions {
		if _, ok := td.Partitions[p]; !ok {
			return res, fmt.Errorf("%w: partition %d does not exist (topic has %d)", ErrInvalid, p, len(td.Partitions))
		}
	}

	committed, err := c.adm.FetchOffsets(ctx, group)
	if err != nil {
		return res, adminErr("fetching offsets of", groupSubject(group), err, "")
	}
	start, end, err := c.offsets(ctx, req.Topic)
	if err != nil {
		return res, err
	}
	var at kadm.ListedOffsets
	if req.To == ResetTimestamp {
		if at, err = c.adm.ListOffsetsAfterMilli(ctx, *req.Timestamp, req.Topic); err != nil {
			return res, fmt.Errorf("listing offsets by time: %w", err)
		}
	}

	commit := make(kadm.Offsets)
	for _, p := range td.Partitions.Sorted() {
		id := p.Partition
		if len(req.Partitions) > 0 && !slices.Contains(req.Partitions, id) {
			continue
		}
		ch := OffsetChange{Partition: id, Current: -1, LagBefore: -1}
		if o, ok := start.Lookup(req.Topic, id); ok && o.Err == nil {
			ch.Start = o.Offset
		}
		if o, ok := end.Lookup(req.Topic, id); ok && o.Err == nil {
			ch.End = o.Offset
		}
		if o, ok := committed.Lookup(req.Topic, id); ok && o.Err == nil && o.At >= 0 {
			ch.Current = o.At
			ch.LagBefore = max(ch.End-o.At, 0)
		}

		switch req.To {
		case ResetEarliest:
			ch.Target = ch.Start
		case ResetLatest:
			ch.Target = ch.End
		case ResetTimestamp:
			ch.Target = ch.End // no record at/after the time: nothing left to read
			if o, ok := at.Lookup(req.Topic, id); ok && o.Err == nil && o.Offset >= 0 {
				ch.Target = o.Offset
			}
		case ResetOffset:
			ch.Target = *req.Offset
		case ResetShift:
			if ch.Current < 0 {
				if len(req.Partitions) > 0 {
					return res, fmt.Errorf("%w: partition %d has no committed offset to shift from", ErrInvalid, id)
				}
				ch.Skipped, ch.Target, ch.LagAfter = true, -1, -1
				res.Changes = append(res.Changes, ch)
				continue
			}
			ch.Target = ch.Current + *req.Offset
		}
		ch.Target = min(max(ch.Target, ch.Start), ch.End)
		ch.LagAfter = ch.End - ch.Target
		res.Changes = append(res.Changes, ch)
		commit.Add(kadm.Offset{Topic: req.Topic, Partition: id, At: ch.Target, LeaderEpoch: -1})
	}

	if req.DryRun {
		return res, nil
	}
	if len(commit) == 0 {
		return res, fmt.Errorf("%w: no partition has a committed offset to shift from", ErrInvalid)
	}
	if res.Active {
		return res, kindErr{kind: ErrConflict, msg: fmt.Sprintf(
			"group %q has %d active member(s); stop its consumers before resetting offsets", group, res.Members)}
	}
	resps, err := c.adm.CommitOffsets(ctx, group, commit)
	if err == nil {
		err = resps.Error()
	}
	if err != nil {
		return res, adminErr("committing offsets of", groupSubject(group), err, "")
	}
	res.Applied = true
	return res, nil
}
