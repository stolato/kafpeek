package kafka

import (
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hamba/avro/v2"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/stolato/kafpeek/internal/schemareg"
)

const userAvro = `{"type":"record","name":"User","fields":[{"name":"name","type":"string"},{"name":"age","type":"int"}]}`

func TestBrowseDecodesSchemaRegistryPayloads(t *testing.T) {
	reg := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/schemas/ids/7":
			_ = json.NewEncoder(w).Encode(map[string]string{"schema": userAvro})
		case "/schemas/ids/8":
			w.WriteHeader(http.StatusInternalServerError)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer reg.Close()

	c, cluster, ctx := newTestClient(t, 1, "users")
	dec, err := schemareg.New(schemareg.Config{URL: reg.URL})
	if err != nil {
		t.Fatal(err)
	}
	c.SetSchemaDecoder(dec)

	sch := avro.MustParse(userAvro)
	payload, err := avro.Marshal(sch, map[string]any{"name": "ana", "age": 31})
	if err != nil {
		t.Fatal(err)
	}
	framed := func(id uint32, p []byte) []byte {
		b := make([]byte, 5, 5+len(p))
		binary.BigEndian.PutUint32(b[1:], id)
		return append(b, p...)
	}

	prod, err := kgo.NewClient(kgo.SeedBrokers(cluster.ListenAddrs()...), kgo.DefaultProduceTopic("users"))
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	for _, v := range [][]byte{framed(7, payload), framed(8, payload), []byte(`{"plain":true}`)} {
		if err := prod.ProduceSync(ctx, &kgo.Record{Value: v}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	res, err := c.Browse(ctx, "users", BrowseOptions{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	byOffset := map[int64]Message{}
	for _, m := range res.Messages {
		byOffset[m.Offset] = m
	}

	ok := byOffset[0]
	if ok.ValueSchema == nil || ok.ValueSchema.ID != 7 || ok.ValueSchema.Type != "AVRO" || ok.ValueSchema.Error != "" {
		t.Fatalf("decoded message schema = %+v", ok.ValueSchema)
	}
	if ok.ValueEncoding != "utf8" || *ok.Value != `{"age":31,"name":"ana"}` {
		t.Fatalf("decoded value = %s (%s)", *ok.Value, ok.ValueEncoding)
	}

	broken := byOffset[1]
	if broken.ValueSchema == nil || broken.ValueSchema.Error == "" || broken.ValueSize != len(framed(8, payload)) || *broken.Value != string(framed(8, payload)) {
		t.Fatalf("registry failure should keep raw bytes and report the error: %+v", broken)
	}

	plain := byOffset[2]
	if plain.ValueSchema != nil || *plain.Value != `{"plain":true}` {
		t.Fatalf("plain JSON should be untouched: %+v", plain)
	}
}
