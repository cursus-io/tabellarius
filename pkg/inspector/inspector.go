package inspector

import (
	"context"
)

type Inspector[T any] interface {
	Start(ctx context.Context, out chan<- T) error
}

type tableMeta struct {
	pkName         string
	pkIndex        int
	columns        []string
	includeColumns map[string]struct{}
	excludeColumns map[string]struct{}
}

func NewTableMeta(pk string) *tableMeta {
	return &tableMeta{
		pkName:  pk,
		pkIndex: -1,
	}
}
