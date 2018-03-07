package main

import "C"

//export cxo_decode_registry
func DecodeRegistry(bytes C.string) (r C.CXORegistry, err C.GoError) {
	//
}

//export cxo_new_registry
func NewRegistry(cl func(t *Reg)) (r *Registry) {
	//
}

//export cxo_registry_encode
func (r *Registry) Encode() []byte {
	//
}

//export cxo_registry_reference
func (r *Registry) Reference() RegistryRef {
	//
}

//export cxo_regisrty_schema_by_name
func (r *Registry) SchemaByName(name string) (Schema, error) {
	//
}

//export cxo_registry_schema_by_reference
func (r *Registry) SchemaByReference(sr SchemaRef) (s Schema, err error) {
	//
}

//
// export cxo_registry_types
// func (r *Registry) Types() (ts *Types)
