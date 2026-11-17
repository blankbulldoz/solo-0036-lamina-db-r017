package journal

import (
	"lamina/internal/catalog"
	"lamina/internal/versions"
)

type Record struct {
	Format      int                 `json:"format"`
	Sequence    uint64              `json:"sequence"`
	Kind        string              `json:"kind"`
	Transaction string              `json:"transaction,omitempty"`
	Relation    *catalog.Relation   `json:"relation,omitempty"`
	Index       *catalog.Index      `json:"index,omitempty"`
	Writes      []versions.Mutation `json:"writes,omitempty"`
	Horizon     uint64              `json:"horizon,omitempty"`
	Request     *RequestIdentity    `json:"request,omitempty"`
}

// RequestIdentity binds a client-supplied commit identity to the exact logical
// write set that was durably published. The fingerprint is computed over the
// canonical mutation payload, so one identity can never silently represent a
// different write. Only commit records that actually published writes carry it.
type RequestIdentity struct {
	ID          string `json:"id"`
	Fingerprint string `json:"fingerprint"`
}
