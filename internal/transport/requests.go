package transport

import (
	"encoding/json"
	"io"
	"lamina/internal/engine"
	"net/http"
)

// decodeOptional behaves like decode but tolerates an empty request body,
// leaving the target at its zero value. Used by endpoints whose body was
// historically optional.
func decodeOptional(w http.ResponseWriter, r *http.Request, v any) bool {
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20)
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		if err == io.EOF {
			return true
		}
		respondError(w, &engine.Error{Code: "invalid", Message: "invalid request: " + err.Error()})
		return false
	}
	var trailing any
	if err := d.Decode(&trailing); err != io.EOF {
		respondError(w, &engine.Error{Code: "invalid", Message: "exactly one JSON value required"})
		return false
	}
	return true
}

func (s *Server) requestStatus(w http.ResponseWriter, r *http.Request) {
	out, err := s.db.RequestStatus(r.PathValue("request"))
	if err != nil {
		respondError(w, err)
		return
	}
	write(w, 200, out)
}
