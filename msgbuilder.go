package mead

import "github.com/kode4food/ale/data"

type (
	// ObjectBuilder is used to construct object values which can, without issues,
	// be serialized to JSON messages following the rules defined by alejson
	ObjectBuilder struct {
		kv map[data.Value]data.Value
	}
)

// Object returns an ObjectBuilder
func Object() *ObjectBuilder {
	return &ObjectBuilder{
		kv: make(map[data.Value]data.Value),
	}
}

// SetString v to field k
func (o *ObjectBuilder) SetString(k, v string) *ObjectBuilder {
	o.kv[data.Keyword(k)] = data.String(v)
	return o
}
