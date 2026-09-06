package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/keshavsaini0311/raftkv/raft"
)

// These run REAL servers over REAL HTTP with REAL fsyncs, in-process.
//
// sim/ proves the algorithm; this proves the wiring — the goroutine boundary,
// the HTTP handlers, the persist-then-send ordering against an actual file.
// They fail in completely different ways, which is why both exist.

// freePorts reserves n ports by binding and immediately releasing them.
// Racy in principle; in practice the window is microseconds and it beats
// hard-coding ports that collide with whatever else is running.
func freePorts(t *testing.T, n int) []int {
	t.Helper()
	out := make([]int, 0, n)
	for i := 0; i < n; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserving a port: %v", err)
		}
		out = append(out, l.Addr().(*net.TCPAddr).Port)
		l.Close()
	}
	return out
}

type testCluster struct {
	t       *testing.T
	servers map[raft.NodeID]*Server
	apis    map[raft.NodeID]string
	ids     []raft.NodeID
}

func newTestCluster(t *testing.T, n int) *testCluster {
	t.Helper()

	ports := freePorts(t, n*2)
	raftAddr := make(map[raft.NodeID]string, n)
	apiAddr := make(map[raft.NodeID]string, n)
	for i := 0; i < n; i++ {
		id := raft.NodeID(i + 1)
		raftAddr[id] = fmt.Sprintf("127.0.0.1:%d", ports[i])
		apiAddr[id] = fmt.Sprintf("127.0.0.1:%d", ports[n+i])
	}

	tc := &testCluster{
		t:       t,
		servers: make(map[raft.NodeID]*Server, n),
		apis:    apiAddr,
	}

	dir := t.TempDir()
	// Quiet: a passing test should print nothing, and raft is chatty under
	// election churn.
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	for i := 0; i < n; i++ {
		id := raft.NodeID(i + 1)
		tc.ids = append(tc.ids, id)

		peers := make(map[raft.NodeID]string, n-1)
		for j := 0; j < n; j++ {
			other := raft.NodeID(j + 1)
			if other != id {
				peers[other] = "http://" + raftAddr[other]
			}
		}

		srv, err := New(Config{
			ID: id, Peers: peers, DataDir: dir,
			RaftAddr: raftAddr[id], APIAddr: apiAddr[id],
			TickInterval:   15 * time.Millisecond,
			Seed:           int64(id),
			RequestTimeout: 3 * time.Second,
			Logger:         logger,
		})
		if err != nil {
			t.Fatalf("New(node %d): %v", id, err)
		}
		tc.servers[id] = srv
	}

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	for _, id := range tc.ids {
		if err := tc.servers[id].Start(ctx); err != nil {
			t.Fatalf("Start(node %d): %v", id, err)
		}
	}
	t.Cleanup(func() {
		for _, id := range tc.ids {
			tc.servers[id].Stop()
		}
	})
	return tc
}

// waitForLeader polls rather than sleeping. A fixed sleep is a flake with a
// delay attached.
func (tc *testCluster) waitForLeader(d time.Duration) raft.NodeID {
	tc.t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		for _, id := range tc.ids {
			if role, _, _ := tc.servers[id].Status(); role == raft.Leader {
				return id
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	tc.t.Fatalf("no leader within %s", d)
	return 0
}

func (tc *testCluster) do(method string, id raft.NodeID, path, body string) (int, string) {
	tc.t.Helper()
	var r io.Reader
	if body != "" {
		r = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, "http://"+tc.apis[id]+path, r)
	if err != nil {
		tc.t.Fatalf("building request: %v", err)
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		tc.t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(b)
}

func TestClusterElectsAndReplicates(t *testing.T) {
	tc := newTestCluster(t, 3)
	leader := tc.waitForLeader(10 * time.Second)

	if code, _ := tc.do(http.MethodPut, leader, "/kv/greeting", "hello"); code != http.StatusOK {
		t.Fatalf("PUT returned %d, want 200", code)
	}

	code, body := tc.do(http.MethodGet, leader, "/kv/greeting", "")
	if code != http.StatusOK || body != "hello" {
		t.Errorf("GET = %d %q, want 200 \"hello\"", code, body)
	}

	// The write must reach every node, not just the leader.
	deadline := time.Now().Add(5 * time.Second)
	for _, id := range tc.ids {
		for time.Now().Before(deadline) {
			if _, keys := tc.do(http.MethodGet, id, "/keys", ""); strings.Contains(keys, "greeting") {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if _, keys := tc.do(http.MethodGet, id, "/keys", ""); !strings.Contains(keys, "greeting") {
			t.Errorf("node %d never received the write (keys: %s)", id, keys)
		}
	}
}

// A follower must refuse writes and say where to go, rather than accepting
// them or redirecting automatically. An automatic redirect would replay the
// body without the client's session bookkeeping noticing, turning a retry into
// a duplicate write.
func TestFollowerRefusesWritesAndNamesTheLeader(t *testing.T) {
	tc := newTestCluster(t, 3)
	leader := tc.waitForLeader(10 * time.Second)

	var follower raft.NodeID
	for _, id := range tc.ids {
		if id != leader {
			follower = id
			break
		}
	}

	code, body := tc.do(http.MethodPut, follower, "/kv/k", "v")
	if code != http.StatusMisdirectedRequest {
		t.Errorf("follower PUT returned %d, want 421", code)
	}
	if !strings.Contains(body, "not the leader") {
		t.Errorf("body = %q, want it to say the node is not the leader", body)
	}
}

// Exactly-once: the same (client, seq) applied twice must not apply twice.
func TestClientSessionsDeduplicateOverHTTP(t *testing.T) {
	tc := newTestCluster(t, 3)
	leader := tc.waitForLeader(10 * time.Second)

	put := func(seq, val string) int {
		t.Helper()
		req, _ := http.NewRequest(http.MethodPut,
			"http://"+tc.apis[leader]+"/kv/counter", strings.NewReader(val))
		req.Header.Set("X-Client-ID", "42")
		req.Header.Set("X-Client-Seq", seq)
		resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
		if err != nil {
			t.Fatalf("PUT: %v", err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	put("1", "first")
	put("1", "second") // a retry of the same request, with different bytes

	_, body := tc.do(http.MethodGet, leader, "/kv/counter", "")
	if body != "first" {
		t.Errorf("value = %q, want \"first\" — a duplicate sequence number was applied", body)
	}

	put("2", "second") // genuinely new
	if _, body := tc.do(http.MethodGet, leader, "/kv/counter", ""); body != "second" {
		t.Errorf("value = %q, want \"second\"", body)
	}
}

func TestMissingKeyIs404(t *testing.T) {
	tc := newTestCluster(t, 1)
	leader := tc.waitForLeader(10 * time.Second)
	if code, _ := tc.do(http.MethodGet, leader, "/kv/absent", ""); code != http.StatusNotFound {
		t.Errorf("GET of a missing key returned %d, want 404", code)
	}
}

// State written before a restart must survive it, recovered from the log file
// alone. This is the fsync contract end to end.
func TestStateSurvivesRestart(t *testing.T) {
	dir := t.TempDir()
	ports := freePorts(t, 2)
	logger := slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))

	cfg := Config{
		ID: 1, Peers: nil, DataDir: dir,
		RaftAddr:       fmt.Sprintf("127.0.0.1:%d", ports[0]),
		APIAddr:        fmt.Sprintf("127.0.0.1:%d", ports[1]),
		TickInterval:   15 * time.Millisecond,
		Seed:           1,
		RequestTimeout: 3 * time.Second,
		Logger:         logger,
	}

	srv, err := New(cfg)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := srv.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}

	tc := &testCluster{t: t, servers: map[raft.NodeID]*Server{1: srv},
		apis: map[raft.NodeID]string{1: cfg.APIAddr}, ids: []raft.NodeID{1}}
	tc.waitForLeader(10 * time.Second)

	if code, _ := tc.do(http.MethodPut, 1, "/kv/durable", "survives"); code != http.StatusOK {
		t.Fatal("initial write failed")
	}
	srv.Stop()
	cancel()

	// The log file must actually exist on disk.
	if _, err := os.Stat(fmt.Sprintf("%s/raft-1.log", dir)); err != nil {
		t.Fatalf("no log file after a clean stop: %v", err)
	}

	// Restart on new ports, same data directory.
	ports2 := freePorts(t, 2)
	cfg.RaftAddr = fmt.Sprintf("127.0.0.1:%d", ports2[0])
	cfg.APIAddr = fmt.Sprintf("127.0.0.1:%d", ports2[1])

	srv2, err := New(cfg)
	if err != nil {
		t.Fatalf("New after restart: %v", err)
	}
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	if err := srv2.Start(ctx2); err != nil {
		t.Fatalf("Start after restart: %v", err)
	}
	defer srv2.Stop()

	tc2 := &testCluster{t: t, servers: map[raft.NodeID]*Server{1: srv2},
		apis: map[raft.NodeID]string{1: cfg.APIAddr}, ids: []raft.NodeID{1}}
	tc2.waitForLeader(10 * time.Second)

	if _, body := tc2.do(http.MethodGet, 1, "/kv/durable", ""); body != "survives" {
		t.Errorf("value after restart = %q, want \"survives\" — durable state was lost", body)
	}
}

func TestParsePeers(t *testing.T) {
	// Not exported, but this is an internal test, and a malformed -peers flag
	// is the most likely operator error at startup.
	tests := []struct {
		in      string
		want    int
		wantErr bool
	}{
		{"", 0, false},
		{"2=http://a:1,3=http://b:2", 2, false},
		{" 2=http://a:1 , 3=http://b:2 ", 2, false},
		{"2=http://a:1/", 1, false}, // trailing slash trimmed
		{"nope", 0, true},
		{"0=http://a:1", 0, true}, // id 0 is reserved for None
		{"x=http://a:1", 0, true},
	}
	for _, tt := range tests {
		got, err := ParsePeers(tt.in)
		if tt.wantErr {
			if err == nil {
				t.Errorf("ParsePeers(%q) = %v, want an error", tt.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("ParsePeers(%q): %v", tt.in, err)
			continue
		}
		if len(got) != tt.want {
			t.Errorf("ParsePeers(%q) returned %d peers, want %d", tt.in, len(got), tt.want)
		}
	}
}

func TestTokenRoundTrip(t *testing.T) {
	for _, v := range []uint64{0, 1, 255, 256, 1 << 40, ^uint64(0)} {
		if got := decodeToken(encodeToken(v)); got != v {
			t.Errorf("decodeToken(encodeToken(%d)) = %d", v, got)
		}
	}
	if got := decodeToken([]byte{1, 2}); got != 0 {
		t.Errorf("decodeToken of a short slice = %d, want 0", got)
	}
}
