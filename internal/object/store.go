package object

import "context"

type Store interface {
	Get(ctx context.Context, uri string) ([]byte, error)
	PutJSON(ctx context.Context, key string, v any) (string, error)
	PutText(ctx context.Context, key string, text string) (string, error)
	PutBytes(ctx context.Context, key string, body []byte, contentType string) (string, error)
	// Exists reports whether an object exists at uri. Used by
	// idempotent-write stages (archive-result, ...) to short-circuit a
	// rerun without re-issuing PutObject. A non-nil error is a transport
	// failure, NOT an "object missing" — implementations MUST map "not
	// found" to (false, nil).
	Exists(ctx context.Context, uri string) (bool, error)
	// URIFor returns the URI at which key WOULD be materialised by a
	// subsequent PutBytes / PutJSON / PutText call — deterministic and
	// side-effect free. Callers use it in combination with Exists to
	// probe "have I already written the deterministic-key artifact for
	// this job?" before spending a PutObject.
	URIFor(key string) string
}
