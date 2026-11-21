package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	// MaxSearchMatches bounds how many matches one search streams to a browser.
	MaxSearchMatches = 10_000
	// searchIdle bounds a single poll, so cancellation and stall checks run
	// regularly even when the broker sends nothing.
	searchIdle = 4 * time.Second
	// searchStall ends a search that made no progress for this long. The
	// partitions still open are reported as unread (reason "incomplete")
	// instead of being counted as covered.
	searchStall = 30 * time.Second
	// progressEvery reports progress mid-batch (a single poll can hold many records).
	progressEvery = 5000
	// searchMaxDuration is a safety net for whole-topic scans.
	searchMaxDuration = 15 * time.Minute
)

// SearchOptions selects which records to scan. Exactly one range mode applies:
// All, a time window (From/To), or the newest Scan records (default).
type SearchOptions struct {
	Partition *int32              // nil = all partitions
	Match     func(*Message) bool // nil = every record matches
	Limit     int                 // stop after this many matches

	All      bool      // scan the whole retained topic
	From, To time.Time // time window; zero values are open ends
	Scan     int       // newest mode: total records to scan across partitions
}

type SearchProgress struct {
	Scanned  int64 `json:"scanned"`  // records read
	Position int64 `json:"position"` // offsets covered so far (for the progress bar)
	Total    int64 `json:"total"`    // offsets in the planned range
	Matched  int   `json:"matched"`
}

// Search reasons.
const (
	ReasonFinished = "finished" // the whole planned range was read
	ReasonLimit    = "limit"    // Limit matches reached
	ReasonTimeout  = "timeout"  // safety duration exceeded
	// ReasonIncomplete: the broker stopped delivering records before the
	// planned range was read; Position < Total tells how much was skipped.
	ReasonIncomplete = "incomplete"
)

type SearchResult struct {
	SearchProgress
	Reason string `json:"reason"`
}

type SearchCallbacks struct {
	OnStart    func(total int64)
	OnMatch    func(Message) error // returning an error aborts the search
	OnProgress func(SearchProgress)
}

type span struct{ start, end int64 } // [start, end)

// Search streams matching records to cb.OnMatch as they are found, scanning
// forward through the planned offset range. Nothing is buffered server-side,
// so memory stays flat regardless of how much is scanned. It never joins a
// consumer group and never commits offsets.
func (c *Client) Search(ctx context.Context, topic string, opts SearchOptions, cb SearchCallbacks) (SearchResult, error) {
	if opts.Limit <= 0 || opts.Limit > MaxSearchMatches {
		opts.Limit = 1000
	}
	spans, err := c.planSearch(ctx, topic, opts)
	if err != nil {
		return SearchResult{}, err
	}
	var res SearchResult
	for _, s := range spans {
		res.Total += s.end - s.start
	}
	if cb.OnStart != nil {
		cb.OnStart(res.Total)
	}
	if len(spans) == 0 {
		res.Reason = ReasonFinished
		return res, nil
	}

	assign := make(map[int32]kgo.Offset, len(spans))
	next := make(map[int32]int64, len(spans)) // next offset to read per partition
	for p, s := range spans {
		assign[p] = kgo.NewOffset().At(s.start)
		next[p] = s.start
	}
	// Control records (transaction markers) are kept so the read position
	// advances through them; otherwise a partition ending in a marker would
	// never visibly reach its end offset.
	cons, err := c.newConsumer(
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: assign}),
		kgo.KeepControlRecords(),
	)
	if err != nil {
		return res, err
	}
	defer cons.Close()

	ctx, cancel := context.WithTimeout(ctx, searchMaxDuration)
	defer cancel()

	position := func() int64 {
		var n int64
		for p, s := range spans {
			n += min(next[p], s.end) - s.start
		}
		return n
	}
	pending := len(spans)
	lastProgress := time.Now()
	for pending > 0 {
		pollCtx, pcancel := context.WithTimeout(ctx, searchIdle)
		fetches := cons.PollFetches(pollCtx)
		pcancel()
		if err := ctx.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				res.Reason = ReasonTimeout
				break
			}
			return res, err // client went away / cancelled
		}

		got := 0
		var done []int32
		var stop error
		fetches.EachRecord(func(r *kgo.Record) {
			if stop != nil {
				return
			}
			s, ok := spans[r.Partition]
			if !ok || next[r.Partition] >= s.end {
				return // partition already finished
			}
			if r.Offset >= s.end { // jumped past the range (markers/compaction): done
				next[r.Partition] = s.end
				done = append(done, r.Partition)
				return
			}
			got++
			next[r.Partition] = r.Offset + 1
			if next[r.Partition] >= s.end {
				done = append(done, r.Partition)
			}
			if r.Attrs.IsControl() {
				return // transaction marker: advances the position, not a message
			}
			res.Scanned++
			if res.Scanned%progressEvery == 0 && cb.OnProgress != nil {
				res.Position = position() // keep counters in step with matches already streamed
				cb.OnProgress(res.SearchProgress)
			}
			m := toMessage(r)
			if opts.Match != nil && !opts.Match(&m) {
				return
			}
			res.Matched++
			if cb.OnProgress != nil {
				res.Position = position()
				cb.OnProgress(res.SearchProgress)
			}
			if err := cb.OnMatch(m); err != nil {
				stop = err
				return
			}
			if res.Matched >= opts.Limit {
				stop = errLimit
			}
		})
		for _, fe := range fetches.Errors() {
			if !errors.Is(fe.Err, context.DeadlineExceeded) && !errors.Is(fe.Err, context.Canceled) {
				return res, fmt.Errorf("partition %d: %w", fe.Partition, fe.Err)
			}
		}
		if len(done) > 0 {
			pending -= len(done)
			cons.PauseFetchPartitions(map[string][]int32{topic: done}) // don't fetch beyond the range
		}
		res.Position = position()
		if cb.OnProgress != nil {
			cb.OnProgress(res.SearchProgress)
		}
		if errors.Is(stop, errLimit) {
			res.Reason = ReasonLimit
			return res, nil
		}
		if stop != nil {
			return res, stop
		}
		if got > 0 {
			lastProgress = time.Now()
			continue
		}
		if time.Since(lastProgress) < searchStall {
			continue // slow broker or long fetch wait: keep polling
		}
		// Stalled. Partitions whose remaining records were deleted by
		// retention meanwhile are done; anything else is reported as unread.
		pending -= c.dropExpired(ctx, topic, spans, next)
		if pending > 0 {
			res.Reason = ReasonIncomplete
			res.Position = position()
			break
		}
	}
	if res.Reason == "" {
		res.Reason = ReasonFinished
		res.Position = res.Total
	}
	return res, nil
}

var errLimit = errors.New("limit reached")

// dropExpired marks open partitions whose log start offset has moved past the
// planned end (records deleted by retention during the scan) as finished and
// returns how many were closed.
func (c *Client) dropExpired(ctx context.Context, topic string, spans map[int32]span, next map[int32]int64) int {
	starts, err := c.adm.ListStartOffsets(ctx, topic)
	if err != nil {
		return 0
	}
	closed := 0
	for p, s := range spans {
		if next[p] >= s.end {
			continue
		}
		if o, ok := starts.Lookup(topic, p); ok && o.Err == nil && o.Offset >= s.end {
			next[p] = s.end
			closed++
		}
	}
	return closed
}

// planSearch turns the options into per-partition offset spans.
func (c *Client) planSearch(ctx context.Context, topic string, opts SearchOptions) (map[int32]span, error) {
	start, end, err := c.offsets(ctx, topic)
	if err != nil {
		return nil, err
	}
	ends, ok := end[topic]
	if !ok {
		return nil, fmt.Errorf("topic %q: %w", topic, ErrNotFound)
	}
	if opts.Partition != nil {
		if _, ok := ends[*opts.Partition]; !ok {
			return nil, fmt.Errorf("%w: partition %d does not exist", ErrInvalid, *opts.Partition)
		}
	}

	var from, to kadm.ListedOffsets
	if !opts.From.IsZero() {
		if from, err = c.adm.ListOffsetsAfterMilli(ctx, opts.From.UnixMilli(), topic); err != nil {
			return nil, fmt.Errorf("resolving start time: %w", err)
		}
	}
	if !opts.To.IsZero() {
		if to, err = c.adm.ListOffsetsAfterMilli(ctx, opts.To.UnixMilli(), topic); err != nil {
			return nil, fmt.Errorf("resolving end time: %w", err)
		}
	}

	selected := 0
	for p := range ends {
		if opts.Partition == nil || *opts.Partition == p {
			selected++
		}
	}
	timeMode := !opts.From.IsZero() || !opts.To.IsZero()
	depth := int64(0)
	if !opts.All && !timeMode {
		scan := opts.Scan
		if scan <= 0 || scan > MaxScan {
			scan = 10_000
		}
		depth = int64((scan + selected - 1) / max(selected, 1))
	}

	spans := map[int32]span{}
	for p, eo := range ends {
		if opts.Partition != nil && *opts.Partition != p {
			continue
		}
		so, ok := start.Lookup(topic, p)
		if eo.Err != nil || !ok || so.Err != nil {
			continue
		}
		s := span{start: so.Offset, end: eo.Offset}
		switch {
		case opts.All:
		case timeMode:
			if o, ok := from.Lookup(topic, p); ok && o.Err == nil && o.Offset >= 0 {
				s.start = max(s.start, o.Offset)
			}
			if o, ok := to.Lookup(topic, p); ok && o.Err == nil && o.Offset >= 0 {
				s.end = min(s.end, o.Offset)
			}
		default:
			s.start = max(s.start, s.end-depth)
		}
		if s.end > s.start {
			spans[p] = s
		}
	}
	return spans, nil
}
