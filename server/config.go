package server

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/keshavsaini0311/raftkv/raft"
)

// ParsePeers reads a "2=http://host:9002,3=http://host:9003" peer list.
//
// It lives here rather than in main so it can be tested: a malformed -peers
// flag is the most likely operator error at startup, and the failure mode
// without validation is a node that starts, elects nobody, and gives no reason.
func ParsePeers(s string) (map[raft.NodeID]string, error) {
	peers := make(map[raft.NodeID]string)
	if strings.TrimSpace(s) == "" {
		return peers, nil // a single-node cluster is legal
	}
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		idStr, url, ok := strings.Cut(part, "=")
		if !ok {
			return nil, fmt.Errorf("peer %q: want id=url", part)
		}
		id, err := strconv.ParseUint(strings.TrimSpace(idStr), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("peer %q: bad id: %w", part, err)
		}
		if id == 0 {
			return nil, fmt.Errorf("peer %q: id 0 is reserved to mean None", part)
		}
		peers[raft.NodeID(id)] = strings.TrimRight(strings.TrimSpace(url), "/")
	}
	return peers, nil
}
