// Package msg represents node messages
package msg

import (
	"errors"
	"fmt"
	"reflect"

	"github.com/skycoin/skycoin/src/cipher"
	"github.com/skycoin/skycoin/src/cipher/encoder"
)

// protocol
//
// [1 byte] - type
// [ .... ] - encoded message
//

// Version is current protocol version
const Version uint16 = 4

// Features of a node
type Features uint64

// features
const (
	// CreatedHashes is feature that pushes hashes of
	// created objects with new created Root.
	CreatedHashes Features = 1 << iota
	// CreatedObjects (mutual exclusive with the CreatedHashes feature)
	// puses created objects instead of hashes
	CreatedObjects
)

// Validate the Features
func (f Features) Validate() (err error) {

	if f == 0 {
		return
	}

	if f&CreatedHashes != 0 && f&CreatedObjects != 0 {
		return errors.New("mutual exclusive features")
	}

	if f&^(CreatedHashes|CreatedObjects) != 0 {
		return errors.New("unknown features")
	}

	return
}

// String implements fmt.Stringer interface and
// returns human-readable list of features
func (f Features) String() (fts string) {

	if f == 0 {
		return "no"
	}

	if f&CreatedHashes != 0 {
		return "created_hashes"
	}

	if f&CreatedObjects != 0 {
		return "created_objects"
	}

	return fmt.Sprintf("unknown features %b", f)
}

// Set implements flag.Value interface
func (f *Features) Set(feat string) (err error) {

	switch feat {
	case "no":
		*f = 0
	case "created_hashes":
		if (*f)&CreatedObjects != 0 {
			return errors.New("mutual exclusive features")
		}
		*f = *f | CreatedHashes
	case "created_objects":
		if (*f)&CreatedHashes != 0 {
			return errors.New("mutual exclusive features")
		}
		*f = *f | CreatedObjects
	default:
		err = errors.New("unknow feature: " + feat)
	}

	return
}

// be sure that all messages implements Msg interface compiler time
var (

	// handshake

	_ Msg = &Syn{} // <- Syn (node id, protocol version, features, data)
	_ Msg = &Ack{} // -> Ack (peer id, features, data)

	// common replies

	_ Msg = &Ok{}  // -> Ok ()
	_ Msg = &Err{} // -> Err (error message)

	// subscriptions

	_ Msg = &Sub{}   // <- Sub (feed)
	_ Msg = &Unsub{} // <- Unsub (feed)

	// public server features

	_ Msg = &RqList{} // <- RqList ()
	_ Msg = &List{}   // -> Lsit  (feeds)

	// root (push and done)

	_ Msg = &Root{} // <- Root (feed, nonce, seq, sig, val)

	// object

	_ Msg = &RqObject{} // <- RqO (key)
	_ Msg = &Object{}   // -> O   (val)

	// objects

	_ Msg = &RqObjects{} // <- RqOs (keys)
	_ Msg = &Objects{}   // -> Os (vals)

	// preview

	_ Msg = &RqPreview{} // -> RqPreview (feed)
)

//
// Msg interface, MsgCore and messages
//

// A Msg is common interface for CXO messages
type Msg interface {
	Type() Type     // type of the message to encode
	Encode() []byte // encode the message to []byte prefixed with the Type
}

//
// handshake
//

// A Syn is handshake initiator message
type Syn struct {
	Protocol uint16        // version
	NodeID   cipher.PubKey // node id
	Features Features      // features flags
	Data     []byte        // reserved for future
}

// Type implements Msg interface
func (*Syn) Type() Type { return SynType }

// Encode the Syn
func (s *Syn) Encode() []byte { return encode(s) }

// An Ack is response for the Syn
// if handshake has been accepted.
// Otherwise, the Err returned
type Ack struct {
	NodeID   cipher.PubKey // node id
	Features Features      // features
	Data     []byte        // reserved for future
}

// Type implements Msg interface
func (*Ack) Type() Type { return AckType }

// Encode the Ack
func (a *Ack) Encode() []byte { return encode(a) }

//
// common
//

// An Ok is common success reply
type Ok struct{}

// Type implements Msg interface
func (*Ok) Type() Type { return OkType }

// Encode the Ok
func (*Ok) Encode() []byte {
	return []byte{
		byte(OkType),
	}
}

// A Err is common error reply
type Err struct {
	Err string // reason
}

// Type implements Msg interface
func (*Err) Type() Type { return ErrType }

// Encode the Err
func (e *Err) Encode() []byte { return encode(e) }

//
// subscriptions
//

// A Sub message is request for subscription
type Sub struct {
	Feed cipher.PubKey
}

// Type implements Msg interface
func (*Sub) Type() Type { return SubType }

// Encode the Sub
func (s *Sub) Encode() []byte {
	return append(
		[]byte{
			byte(SubType),
		},
		s.Feed[:]...,
	)
}

// An Unsub used to notify remote peer about
// unsubscribing from a feed
type Unsub struct {
	Feed cipher.PubKey
}

// Type implements Msg interface
func (*Unsub) Type() Type { return UnsubType }

// Encode the Unsub
func (u *Unsub) Encode() []byte {
	return append(
		[]byte{
			byte(UnsubType),
		},
		u.Feed[:]...,
	)
}

//
// list of feeds
//

// A RqList is request of list of feeds
type RqList struct{}

// Type implements Msg interface
func (*RqList) Type() Type { return RqListType }

// Encode the RqList
func (*RqList) Encode() []byte {
	return []byte{
		byte(RqListType),
	}
}

// A List is reply for RqList
type List struct {
	Feeds []cipher.PubKey
}

// Type implements Msg interface
func (*List) Type() Type { return ListType }

// Encode the List
func (l *List) Encode() []byte { return encode(l) }

//
// root (push an done)
//

// A Root sent from one node to another one
// to update root object of feed described in
// Feed field of the message
type Root struct {
	Feed  cipher.PubKey // feed }
	Nonce uint64        // head } Root selector
	Seq   uint64        // seq  }

	Value []byte // encoded Root in person

	Sig cipher.Sig // signature

	// optional fields, that depends on features

	CreatedHashes  []cipher.SHA256 // hashes of created objects
	CreatedObjects [][]byte        // created objects
}

// Type implements Msg interface
func (*Root) Type() Type { return RootType }

// Encode the Root
func (r *Root) Encode() []byte { return encode(r) }

//
// object
//

// A RqObject represents a Msg that request a data by hash
type RqObject struct {
	Key cipher.SHA256 // request
}

// Type implements Msg interface
func (*RqObject) Type() Type { return RqObjectType }

// Encode the RqObject
func (r *RqObject) Encode() []byte { return encode(r) }

// An Object reperesents encoded object
type Object struct {
	Value []byte // encoded object in person
}

// Type implements Msg interface
func (*Object) Type() Type { return ObjectType }

// Encode the Object
func (o *Object) Encode() []byte { return encode(o) }

//
// objects
//

// A RqObjects represents a Msg that request a data
// by list of hashes. The request objects used to request
// many small obejcts. A peer returns all it can. E.g.
// by this request it sends all obejcts before first
// not found objects. And before skyobejct.MaxObjectSize
// limit. Thus, the request is optimistic and can be
// useful only for many small objects.
type RqObjects struct {
	Keys []cipher.SHA256 // request
}

// Type implements Msg interface
func (*RqObjects) Type() Type { return RqObjectsType }

// Encode the RqObjects
func (r *RqObjects) Encode() []byte { return encode(r) }

// Objects reperesents encoded objects. The messege never
// exceed skyobejct.MaxObjectSize limit. And the messege
// can contains not all requeted obejcts
type Objects struct {
	Values [][]byte // encoded objects
}

// Type implements Msg interface
func (*Objects) Type() Type { return ObjectsType }

// Encode the Objects
func (o *Objects) Encode() []byte { return encode(o) }

//
// preview
//

// RqPreview is request for feeds preview
type RqPreview struct {
	Feed cipher.PubKey
}

// Type implements Msg interface
func (*RqPreview) Type() Type { return RqPreviewType }

// Encode the RqPreview
func (r *RqPreview) Encode() []byte { return encode(r) }

//
// Type / Encode / Deocode / String()
//

// A Type represent msg prefix
type Type uint8

// Types
const (
	SynType = 1 + iota // 1
	AckType            // 2

	OkType  // 3
	ErrType // 4

	SubType   // 5
	UnsubType // 6

	RqListType // 7
	ListType   // 8

	RootType // 9

	RqObjectType // 10
	ObjectType   // 11

	RqObjectsType // 12
	ObjectsType   // 13

	RqPreviewType // 14
)

// Type to string mapping
var msgTypeString = [...]string{
	SynType: "Syn",
	AckType: "Ack",

	OkType:  "Ok",
	ErrType: "Err",

	SubType:   "Sub",
	UnsubType: "Unsub",

	RqListType: "RqList",
	ListType:   "List",

	RootType: "Root",

	RqObjectType: "RqObject",
	ObjectType:   "Object",

	RqObjectsType: "RqObjects",
	ObjectsType:   "Objects",

	RqPreviewType: "RqPreview",
}

// String implements fmt.Stringer interface
func (m Type) String() string {
	if im := int(m); im > 0 && im < len(msgTypeString) {
		return msgTypeString[im]
	}
	return fmt.Sprintf("Type<%d>", m)
}

var forwardRegistry = [...]reflect.Type{
	SynType: reflect.TypeOf(Syn{}),
	AckType: reflect.TypeOf(Ack{}),

	OkType:  reflect.TypeOf(Ok{}),
	ErrType: reflect.TypeOf(Err{}),

	SubType:   reflect.TypeOf(Sub{}),
	UnsubType: reflect.TypeOf(Unsub{}),

	RqListType: reflect.TypeOf(RqList{}),
	ListType:   reflect.TypeOf(List{}),

	RootType: reflect.TypeOf(Root{}),

	RqObjectType: reflect.TypeOf(RqObject{}),
	ObjectType:   reflect.TypeOf(Object{}),

	RqObjectsType: reflect.TypeOf(RqObjects{}),
	ObjectsType:   reflect.TypeOf(Objects{}),

	RqPreviewType: reflect.TypeOf(RqPreview{}),
}

// An InvalidTypeError represents decoding error when
// incoming message is malformed and its type invalid
type InvalidTypeError struct {
	typ Type
}

// Type return Type which cause the error
func (e InvalidTypeError) Type() Type {
	return e.typ
}

// Error implements builting error interface
func (e InvalidTypeError) Error() string {
	return fmt.Sprint("invalid message type: ", e.typ.String())
}

var (
	// ErrEmptyMessage occurs when you
	// try to Decode an empty slice
	ErrEmptyMessage = errors.New("empty message")
	// ErrIncomplieDecoding occurs when incoming message
	// decoded correctly but the decoding doesn't use
	// entire encoded message
	ErrIncomplieDecoding = errors.New("incomplete decoding")
)

// encode given message to []byte prefixed by Type
func encode(msg Msg) (p []byte) {
	p = append(
		[]byte{
			byte(msg.Type()),
		},
		encoder.Serialize(msg)...,
	)
	return
}

// Decode encoded Type-prefixed data to message.
// It can returns encoding errors or InvalidTypeError
func Decode(p []byte) (msg Msg, err error) {

	if len(p) < 1 {
		err = ErrEmptyMessage
		return
	}

	var mt = Type(p[0])

	if mt <= 0 || int(mt) >= len(forwardRegistry) {
		err = InvalidTypeError{mt}
		return
	}

	var (
		typ = forwardRegistry[mt]
		val = reflect.New(typ)

		n int
	)

	if n, err = encoder.DeserializeRawToValue(p[1:], val); err != nil {
		return
	}

	if n+1 != len(p) {
		err = ErrIncomplieDecoding
		return
	}

	msg = val.Interface().(Msg)
	return
}
