package flow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	"github.com/SaiPisey2/cutover/internal/pg"
	"github.com/SaiPisey2/cutover/internal/readiness"
)

// EvidenceDigest fingerprints what a gate decision rests on: the gate with
// its approvals, and the evidence shown with it. Events are left out, so
// log noise does not change it. It is "" when no gate is open. The struct
// fixes the field order, so the encoding is canonical.
func EvidenceDigest(st Status) string {
	if st.Gate == nil {
		return ""
	}
	b, err := json.Marshal(struct {
		Gate         *Gate
		Phase        string
		Readiness    *readiness.Report
		Checks       []pg.TableCheck
		Sequences    []pg.SequenceSync
		Weight       int
		Traffic      []ServiceTraffic
		ShiftVerdict string
	}{st.Gate, st.Phase, st.Readiness, st.Checks, st.Sequences, st.Weight, st.Traffic, st.ShiftVerdict})
	if err != nil { // plain data types: cannot fail
		return ""
	}
	sum := sha256.Sum256(b)
	return hex.EncodeToString(sum[:])
}
