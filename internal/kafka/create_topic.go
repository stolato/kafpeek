package kafka

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
)

// Limits for topics created from the UI.
const (
	MaxTopicNameLen     = 249 // Kafka's own limit
	MaxCreatePartitions = 10000
	MaxCreateConfigs    = 100
)

var (
	// ErrConflict is returned when the resource already exists.
	ErrConflict = errors.New("already exists")
	// ErrForbidden is returned when the cluster denies the operation (ACLs/IAM).
	ErrForbidden = errors.New("not authorized")
)

var topicNameRE = regexp.MustCompile(`^[a-zA-Z0-9._-]+$`)

type CreateTopicRequest struct {
	Name string `json:"name"`
	// Partitions and ReplicationFactor fall back to the broker defaults
	// (num.partitions / default.replication.factor) when nil.
	Partitions        *int32            `json:"partitions"`
	ReplicationFactor *int16            `json:"replicationFactor"`
	Configs           map[string]string `json:"configs"`
}

type CreateTopicResult struct {
	Name              string `json:"name"`
	ID                string `json:"id,omitempty"`
	Partitions        int32  `json:"partitions"`
	ReplicationFactor int16  `json:"replicationFactor"`
}

// ValidateTopicName applies Kafka's topic naming rules.
func ValidateTopicName(name string) error {
	switch {
	case name == "":
		return fmt.Errorf("%w: topic name is required", ErrInvalid)
	case name == "." || name == "..":
		return fmt.Errorf("%w: topic name cannot be %q", ErrInvalid, name)
	case len(name) > MaxTopicNameLen:
		return fmt.Errorf("%w: topic name is %d characters, max is %d", ErrInvalid, len(name), MaxTopicNameLen)
	case !topicNameRE.MatchString(name):
		return fmt.Errorf("%w: topic name may only contain letters, digits, '.', '_' and '-'", ErrInvalid)
	}
	return nil
}

func (req CreateTopicRequest) validate() error {
	if err := ValidateTopicName(req.Name); err != nil {
		return err
	}
	if p := req.Partitions; p != nil && (*p < 1 || *p > MaxCreatePartitions) {
		return fmt.Errorf("%w: partitions must be between 1 and %d", ErrInvalid, MaxCreatePartitions)
	}
	if rf := req.ReplicationFactor; rf != nil && *rf < 1 {
		return fmt.Errorf("%w: replication factor must be at least 1", ErrInvalid)
	}
	if len(req.Configs) > MaxCreateConfigs {
		return fmt.Errorf("%w: at most %d configs", ErrInvalid, MaxCreateConfigs)
	}
	for k := range req.Configs {
		if strings.TrimSpace(k) == "" {
			return fmt.Errorf("%w: config names cannot be empty", ErrInvalid)
		}
	}
	return nil
}

// CreateTopic creates a topic. Broker-side rejections are mapped to
// ErrConflict (exists), ErrForbidden (ACL/IAM) or ErrInvalid (bad settings).
func (c *Client) CreateTopic(ctx context.Context, req CreateTopicRequest) (CreateTopicResult, error) {
	if c.readOnly {
		return CreateTopicResult{}, ErrReadOnly
	}
	if err := req.validate(); err != nil {
		return CreateTopicResult{}, err
	}

	partitions, rf := int32(-1), int16(-1)
	if req.Partitions != nil {
		partitions = *req.Partitions
	}
	if req.ReplicationFactor != nil {
		rf = *req.ReplicationFactor
	}
	var configs map[string]*string
	if len(req.Configs) > 0 {
		configs = make(map[string]*string, len(req.Configs))
		for k, v := range req.Configs {
			configs[strings.TrimSpace(k)] = &v
		}
	}

	resp, err := c.adm.CreateTopic(ctx, partitions, rf, configs, req.Name)
	if err != nil {
		return CreateTopicResult{}, adminErr("creating", topicSubject(req.Name), err, resp.ErrMessage)
	}
	out := CreateTopicResult{
		Name:              req.Name,
		Partitions:        resp.NumPartitions,
		ReplicationFactor: resp.ReplicationFactor,
	}
	if resp.ID != [16]byte{} {
		out.ID = resp.ID.String()
	}
	return out, nil
}

func topicSubject(name string) string { return fmt.Sprintf("topic %q", name) }
func groupSubject(name string) string { return fmt.Sprintf("group %q", name) }

// adminErr maps broker-side rejections of an admin request to the sentinels
// the API turns into HTTP statuses. action is e.g. "creating"; subject is
// e.g. topicSubject(name).
func adminErr(action, subject string, err error, msg string) error {
	detail := err.Error()
	if msg != "" {
		detail = msg
	}
	var kind error
	switch {
	case errors.Is(err, kerr.UnknownTopicOrPartition):
		kind = ErrNotFound
	case errors.Is(err, kerr.TopicAlreadyExists):
		kind = ErrConflict
	case errors.Is(err, kerr.TopicAuthorizationFailed),
		errors.Is(err, kerr.ClusterAuthorizationFailed),
		errors.Is(err, kerr.GroupAuthorizationFailed),
		errors.Is(err, kerr.TopicDeletionDisabled):
		kind = ErrForbidden
	case errors.Is(err, kerr.InvalidTopicException),
		errors.Is(err, kerr.InvalidPartitions),
		errors.Is(err, kerr.InvalidReplicationFactor),
		errors.Is(err, kerr.InvalidReplicaAssignment),
		errors.Is(err, kerr.InvalidConfig),
		errors.Is(err, kerr.PolicyViolation),
		errors.Is(err, kerr.InvalidRequest),
		errors.Is(err, kerr.OffsetOutOfRange):
		kind = ErrInvalid
	case errors.Is(err, kerr.UnknownMemberID),
		errors.Is(err, kerr.IllegalGeneration),
		errors.Is(err, kerr.RebalanceInProgress),
		errors.Is(err, kerr.NonEmptyGroup):
		kind = ErrConflict
	default:
		return fmt.Errorf("%s %s: %w", action, subject, err)
	}
	return kindErr{kind: kind, msg: subject + ": " + detail}
}

// kindErr carries a sentinel (for the HTTP status) without repeating it in
// the message shown to the user.
type kindErr struct {
	kind error
	msg  string
}

func (e kindErr) Error() string { return e.msg }
func (e kindErr) Unwrap() error { return e.kind }
