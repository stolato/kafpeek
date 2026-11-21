package kafka

import "testing"

func TestIsInternalTopic(t *testing.T) {
	cases := map[string]bool{
		"__consumer_offsets":          true,
		"__amazon_msk_canary":         true,
		"__amazon_msk_canary_state":   true,
		"_schemas":                    true,
		"__confluent.support.metrics": true,
		"orders":                      false,
		"payments_v2":                 false,
		"my__topic":                   false,
	}
	for name, want := range cases {
		if got := IsInternalTopic(name, false); got != want {
			t.Errorf("IsInternalTopic(%q) = %v, want %v", name, got, want)
		}
	}
	if !IsInternalTopic("weird", true) {
		t.Error("topics flagged internal by Kafka must stay internal")
	}
}
