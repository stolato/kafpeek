package config

import "testing"

// A typo in a boolean switch must fail loudly: KAFKA_READ_ONLY=yes used to
// silently leave writes enabled.
func TestLoadRejectsInvalidBoolean(t *testing.T) {
	t.Setenv("KAFKA_READ_ONLY", "yes")
	if _, err := Load(); err == nil {
		t.Fatal("KAFKA_READ_ONLY=yes must be an error, not read-write")
	}
}

func TestLoadReadOnlySwitch(t *testing.T) {
	for val, want := range map[string]bool{"true": true, "1": true, "false": false, "": false} {
		t.Setenv("KAFKA_READ_ONLY", val)
		cfg, err := Load()
		if err != nil {
			t.Fatalf("KAFKA_READ_ONLY=%q: unexpected error %v", val, err)
		}
		if cfg.Kafka.ReadOnly != want {
			t.Fatalf("KAFKA_READ_ONLY=%q: ReadOnly=%v, want %v", val, cfg.Kafka.ReadOnly, want)
		}
	}
}
