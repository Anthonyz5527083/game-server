package metrics

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/Anthonyz5527083/game-server/internal/pb"
)

func TestMessageCounters(t *testing.T) {
	ping := &pb.Envelope{Payload: &pb.Envelope_Ping{Ping: &pb.Ping{}}}
	snap := &pb.Envelope{Payload: &pb.Envelope_Snapshot{Snapshot: &pb.Snapshot{}}}
	empty := &pb.Envelope{Seq: 3}

	value := func(dir, typ string) float64 { return testutil.ToFloat64(messages.WithLabelValues(dir, typ)) }
	pingIn, snapOut, noneIn := value("in", "ping"), value("out", "snapshot"), value("in", "none")

	Received(ping)
	Received(ping)
	Sent(snap, 8)
	Sent(snap, 0)
	Received(empty)

	if d := value("in", "ping") - pingIn; d != 2 {
		t.Errorf("in/ping += %v, want 2", d)
	}
	if d := value("out", "snapshot") - snapOut; d != 8 {
		t.Errorf("out/snapshot += %v, want 8", d)
	}
	if d := value("in", "none") - noneIn; d != 1 {
		t.Errorf("in/none += %v, want 1", d)
	}
}

// /metrics 能抓到;所有消息类型一开始就在(值为 0);gauge 每次抓取现算;pprof 挂上了。
func TestAdminHandler(t *testing.T) {
	var n atomic.Int64
	SetGaugeSources(func() int { return 3 }, func() int { return int(n.Add(1)) }, func() int { return 1 })

	srv := httptest.NewServer(AdminHandler())
	defer srv.Close()
	body := get(t, srv.URL+"/metrics")
	for _, want := range []string{
		`gameserver_messages_total{dir="in",type="move_input"} 0`,
		`gameserver_messages_total{dir="out",type="room_event"} 0`,
		"gameserver_connections 3",
		"gameserver_players_online 1",
		"gameserver_rooms 1",
		"gameserver_tick_duration_seconds_bucket",
		"go_goroutines",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("/metrics missing %q", want)
		}
	}
	if !strings.Contains(get(t, srv.URL+"/metrics"), "gameserver_players_online 2") {
		t.Error("gauge not recomputed on second scrape")
	}
	if !strings.Contains(get(t, srv.URL+"/debug/pprof/"), "goroutine") {
		t.Error("pprof index not served")
	}
}

func get(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
