package engine

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"lamina/internal/datum"
	"lamina/internal/journal"
	"lamina/internal/versions"
	"sort"
	"strings"
)

// maxRequestID bounds a client-supplied commit identity.
const maxRequestID = 128

// maxRetainedRequests bounds the in-memory identity index independently of the
// durable horizon. Entries are evicted oldest-commit-first, so the bound itself
// is a deterministic, replayable retention floor rather than unbounded state.
const maxRetainedRequests = 100000

// RequestOutcome is the single determined result that one request identity is
// allowed to represent. It is reconstructed only from durable journal records;
// no sidecar file or image field exists, so the identity layer can never invent
// a write outcome that the transaction/constraint machinery did not publish.
type RequestOutcome struct {
	ID          string `json:"request_id"`
	Status      string `json:"status"`
	Sequence    uint64 `json:"sequence"`
	Writes      int    `json:"writes"`
	Fingerprint string `json:"fingerprint"`
}

type requestSeq struct {
	sequence uint64
	id       string
}

// writeFingerprint hashes the canonical exact logical write set. Rows are
// normalized before commit, map keys marshal in sorted order, and the mutation
// slice is sorted here as well, so the same logical write always hashes
// identically regardless of request encoding or staging order.
func writeFingerprint(writes []versions.Mutation) string {
	type canonical struct {
		Relation string    `json:"relation"`
		Key      string    `json:"key"`
		Deleted  bool      `json:"deleted"`
		Row      datum.Row `json:"row,omitempty"`
	}
	ordered := make([]canonical, len(writes))
	for i, m := range writes {
		ordered[i] = canonical{Relation: m.Relation, Key: m.Key, Deleted: m.Deleted, Row: m.Row}
	}
	sort.Slice(ordered, func(i, j int) bool {
		if ordered[i].Relation == ordered[j].Relation {
			return ordered[i].Key < ordered[j].Key
		}
		return ordered[i].Relation < ordered[j].Relation
	})
	payload, _ := json.Marshal(ordered)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

func validRequestID(id string) error {
	if len(id) == 0 || len(id) > maxRequestID {
		return fault("invalid", "request_id must be 1..%d bytes", maxRequestID)
	}
	if id != strings.TrimSpace(id) {
		return fault("invalid", "request_id must not include leading or trailing whitespace")
	}
	for _, r := range id {
		if r < 0x20 || r == 0x7f {
			return fault("invalid", "request_id must not contain control characters")
		}
		// These characters would change URL path/query/fragment interpretation
		// when the identity is later queried via /requests/{request}.
		if r == '/' || r == '?' || r == '#' {
			return fault("invalid", "request_id must not contain /, ? or # characters")
		}
	}
	return nil
}

// registerRequest records the determined result of a durable commit. The first
// binding of an identity wins permanently within the retained range; a
// conflicting replay of journal data is rejected by the caller.
func (d *DB) registerRequest(identity *journal.RequestIdentity, sequence uint64, writes int) {
	if identity == nil {
		return
	}
	if _, exists := d.requests[identity.ID]; exists {
		return
	}
	d.requests[identity.ID] = &RequestOutcome{ID: identity.ID, Status: "committed", Sequence: sequence, Writes: writes, Fingerprint: identity.Fingerprint}
	d.requestOrder = append(d.requestOrder, requestSeq{sequence: sequence, id: identity.ID})
	d.capRequests()
}

// recoverIdentity rebuilds identity state from journal records whose effects
// are already represented by a checkpoint image, and from the replayed suffix
// via apply. Only commit and vacuum records carry identity/retention facts.
func (d *DB) recoverIdentity(r journal.Record) error {
	switch r.Kind {
	case "commit":
		if r.Request != nil {
			if prior, ok := d.requests[r.Request.ID]; ok && prior.Fingerprint != r.Request.Fingerprint {
				return journalIdentityFault(r.Request.ID)
			}
			d.registerRequest(r.Request, r.Sequence, len(r.Writes))
		}
	case "vacuum":
		d.expireRequests(r.Horizon)
	}
	return nil
}

func journalIdentityFault(id string) error {
	return fault("unavailable", "journal binds request identity %q to conflicting payloads", id)
}

// expireRequests drops identities whose commit sequence is at or below the new
// durable vacuum horizon. The horizon cannot move backwards, so an expired
// identity can never become answerable again on this instance.
func (d *DB) expireRequests(horizon uint64) {
	for d.requestHead < len(d.requestOrder) {
		entry := d.requestOrder[d.requestHead]
		if entry.sequence > horizon {
			break
		}
		if outcome, ok := d.requests[entry.id]; ok && outcome.Sequence == entry.sequence {
			delete(d.requests, entry.id)
		}
		d.requestHead++
	}
	d.compactOrder()
}

// capRequests enforces the bounded-memory retention limit. Eviction follows
// commit order and is therefore reproduced identically during replay.
func (d *DB) capRequests() {
	for len(d.requests) > maxRetainedRequests && d.requestHead < len(d.requestOrder) {
		entry := d.requestOrder[d.requestHead]
		if outcome, ok := d.requests[entry.id]; ok && outcome.Sequence == entry.sequence {
			delete(d.requests, entry.id)
		}
		d.requestHead++
	}
	d.compactOrder()
}

func (d *DB) compactOrder() {
	if d.requestHead == 0 {
		return
	}
	d.requestOrder = append([]requestSeq(nil), d.requestOrder[d.requestHead:]...)
	d.requestHead = 0
}

// requestFloor reports the highest sequence whose identity retention is no
// longer guaranteed: vacuum expiration and the memory cap both contribute.
func (d *DB) requestFloor() uint64 {
	floor := d.horizon
	for d.requestHead < len(d.requestOrder) {
		entry := d.requestOrder[d.requestHead]
		if _, ok := d.requests[entry.id]; ok {
			if entry.sequence > 0 && entry.sequence-1 > floor {
				floor = entry.sequence - 1
			}
			break
		}
		d.requestHead++
	}
	d.compactOrder()
	return floor
}

// RequestStatus returns the determined commit result bound to an identity, or a
// not-found fault that also reports the retention floor so a disconnected
// client knows it must reconcile against the committed data instead.
func (d *DB) RequestStatus(id string) (RequestOutcome, error) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	if err := d.usable(); err != nil {
		return RequestOutcome{}, err
	}
	if outcome, ok := d.requests[id]; ok {
		return *outcome, nil
	}
	return RequestOutcome{}, fault("not_found", "request identity unknown or expired; retention floor sequence %d; reconcile against committed data", d.requestFloor())
}
