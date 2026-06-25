package object

import "context"

type Store interface {
	Get(ctx context.Context, uri string) ([]byte, error)
	PutJSON(ctx context.Context, key string, v any) (string, error)
	PutText(ctx context.Context, key string, text string) (string, error)
	PutBytes(ctx context.Context, key string, body []byte, contentType string) (string, error)
}
