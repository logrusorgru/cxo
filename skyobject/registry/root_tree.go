package registry

import (
	"encoding/hex"
	"fmt"
	"reflect"

	"github.com/DiSiqueira/GoTree"

	"github.com/skycoin/skycoin/src/cipher"
	"github.com/skycoin/skycoin/src/cipher/encoder"
)

// Tree used to print Root tree. The forceLoad
// argument used to load subtrees if they are
// not loaded. The Pack should have related
// Registry
func (r *Root) Tree(pack Pack) (tree string) {

	var (
		gt = gotree.New(
			fmt.Sprintf("(root) %s %s (reg: %s)",
				r.Hash.Hex()[:7], r.Short(), r.Reg.Short(),
			),
		)
		reg = pack.Registry()
	)

	if len(r.Refs) == 0 && r.Reg.IsBlank() == true {
		gt.Add("(blank)")
		tree = gt.Print()
		return
	}

	if reg == nil {
		gt.Add("(err) missing registry in Pack")
		tree = gt.Print()
		return
	}

	if len(r.Refs) == 0 {
		gt.Add("(empty)")
	} else {
		for _, dr := range r.Refs {
			rootTreeDynamic(gt, &dr, pack)
		}
	}

	tree = gt.Print()
	return
}

func rootTreeDynamic(
	gt gotree.Tree, // :
	d *Dynamic, //     :
	pack Pack, //      :
) {

	if d.IsValid() == false {
		gt.Add("*(dynamic) err: " + ErrInvalidDynamicReference.Error())
		return
	}

	if d.IsBlank() == true {
		gt.Add("*(dynamic) nil")
		return
	}

	var (
		sch Schema
		err error
	)

	if sch, err = pack.Registry().SchemaByReference(d.Schema); err != nil {
		gt.Add("*(dynamic) err: " + err.Error())
		return
	}

	if d.Hash == (cipher.SHA256{}) {
		gt.Add(fmt.Sprintf("*(dynamic) nil (type %s)", sch.String()))
		return
	}

	var it = gotree.New("*(dynamic) " + d.Short())
	rootTreeHash(it, pack, sch, d.Hash)
	gt.AddTree(it)
}

func rootTreeHash(
	gt gotree.Tree, //     :
	pack Pack, //          :
	sch Schema, //         :
	hash cipher.SHA256, // :
) {

	var (
		val []byte
		err error
	)

	if val, err = pack.Get(hash); err != nil {
		gt.Add("(err) " + err.Error())
		return
	}

	rootTreeData(gt, pack, sch, val)
}

func rootTreeData(
	gt gotree.Tree, // :
	pack Pack, //      :
	sch Schema, //     :
	val []byte, //     :
) {

	if sch.IsReference() == true {
		rootTreeReferences(gt, pack, sch, val)
		return
	}

	switch sch.Kind() {
	case reflect.Bool:
		rootTreeBool(gt, sch, val)
	case reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		rootTreeInt(gt, sch, val)
	case reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
		rootTreeUint(gt, sch, val)
	case reflect.Float32, reflect.Float64:
		rootTreeFloat(gt, sch, val)
	case reflect.String:
		rootTreeString(gt, sch, val)
	case reflect.Array, reflect.Slice:
		rootTreeSlice(gt, pack, sch, val)
	case reflect.Struct:
		rootTreeStruct(gt, pack, sch, val)
	default:
		gt.Add(
			fmt.Sprintf("(err) invalid Kind <%s> of Schema %q",
				sch.Kind().String(),
				sch.String(),
			),
		)
	}
}

func rootTreeReferences(
	gt gotree.Tree, // :
	pack Pack, //      :
	sch Schema, //     :
	val []byte, //     :
) {

	switch rt := sch.ReferenceType(); rt {
	case ReferenceTypeSingle:
		rootTreeRef(gt, pack, sch, val)
	case ReferenceTypeSlice:
		rootTreeRefs(gt, pack, sch, val)
	case ReferenceTypeDynamic:
		var (
			dr  Dynamic
			err error
		)
		if err = encoder.DeserializeRaw(val, &dr); err != nil {
			gt.Add("*(dynamic) err: " + err.Error())
			return
		}
		rootTreeDynamic(gt, &dr, pack)
	default:
		gt.Add(
			fmt.Sprintf(
				"invalid schema (%s): reference with invalid type %d",
				sch.String(),
				rt,
			),
		)
	}
}

func rootTreeValue(
	gt gotree.Tree, //  :
	sch Schema, //      :
	val interface{}, // :
) {
	if name := sch.Name(); name != "" {
		gt.Add(fmt.Sprintf("%v (type %s)", val, name))
		return
	}
	gt.Add(fmt.Sprint(val))
	return

}

func rootTreeBool(
	gt gotree.Tree, // :
	sch Schema, //     :
	val []byte, //     :
) {

	var (
		x   bool
		err error
	)

	if err = encoder.DeserializeRaw(val, &x); err != nil {
		gt.Add("(err) " + err.Error())
		return
	}
	rootTreeValue(gt, sch, x)
}

func rootTreeInt(
	gt gotree.Tree, // :
	sch Schema, //     :
	val []byte, //     :
) {

	var (
		x   int64
		err error
	)

	switch sch.Kind() {
	case reflect.Int8:
		var y int8
		err = encoder.DeserializeRaw(val, &y)
		x = int64(y)
	case reflect.Int16:
		var y int16
		err = encoder.DeserializeRaw(val, &y)
		x = int64(y)
	case reflect.Int32:
		var y int32
		err = encoder.DeserializeRaw(val, &y)
		x = int64(y)
	case reflect.Int64:
		err = encoder.DeserializeRaw(val, &x)
	}

	if err != nil {
		gt.Add("(err) " + err.Error())
		return
	}

	rootTreeValue(gt, sch, x)
}

func rootTreeUint(
	gt gotree.Tree, // :
	sch Schema, //     :
	val []byte, //     :
) {

	var (
		x   uint64
		err error
	)

	switch sch.Kind() {
	case reflect.Uint8:
		var y uint8
		err = encoder.DeserializeRaw(val, &y)
		x = uint64(y)
	case reflect.Uint16:
		var y uint16
		err = encoder.DeserializeRaw(val, &y)
		x = uint64(y)
	case reflect.Uint32:
		var y uint32
		err = encoder.DeserializeRaw(val, &y)
		x = uint64(y)
	case reflect.Uint64:
		err = encoder.DeserializeRaw(val, &x)
	}
	if err != nil {
		gt.Add("(err) " + err.Error())
		return
	}
	rootTreeValue(gt, sch, x)
}

func rootTreeFloat(
	gt gotree.Tree, // :
	sch Schema, //     :
	val []byte, //     :
) {

	var (
		x   float64
		err error
	)

	switch sch.Kind() {
	case reflect.Float32:
		var y float32
		err = encoder.DeserializeRaw(val, &y)
		x = float64(y)
	case reflect.Float64:
		err = encoder.DeserializeRaw(val, &x)
	}

	if err != nil {
		gt.Add("(err) " + err.Error())
		return
	}

	rootTreeValue(gt, sch, x)
}

func rootTreeString(
	gt gotree.Tree, // :
	sch Schema, //     :
	val []byte, //     :
) {

	var (
		x   string
		err error
	)

	if err = encoder.DeserializeRaw(val, &x); err != nil {
		gt.Add("(err) " + err.Error())
		return
	}

	rootTreeValue(gt, sch, x)
}

// slice or array
func rootTreeSlice(
	gt gotree.Tree, // :
	pack Pack, //      :
	sch Schema, //     :
	val []byte, //     :
) {

	var (
		el  Schema
		err error
	)

	if el = sch.Elem(); el == nil {
		gt.Add(
			fmt.Sprintf("(err) invalid schema %q: nil-element", sch.String()),
		)
		return
	}

	// special case for []byte
	if sch.Kind() == reflect.Slice && el.Kind() == reflect.Uint8 {

		var x []byte
		if err = encoder.DeserializeRaw(val, &x); err != nil {
			gt.Add("(err) " + err.Error())
			return
		}

		gt.Add("([]byte) " + hex.EncodeToString(x))
		return
	}

	var (
		ln    int // length
		shift int // shift

		it gotree.Tree
	)

	if sch.Kind() == reflect.Array {

		ln = sch.Len()

		if name := sch.Name(); name != "" {
			it = gotree.New(fmt.Sprintf("[%d]%s (%s)", ln, el.String(), name))
		} else {
			it = gotree.New(fmt.Sprintf("[%d]%s", ln, el.String()))
		}

	} else {

		var itName string

		// reflect.Slice
		if name := sch.Name(); name != "" {
			itName = fmt.Sprintf("[]%s (%s)", el.String(), name)
		} else {
			itName = fmt.Sprintf("[]%s", el.String())
		}

		if ln, err = getLength(val); err != nil {
			gt.Add(" (err) " + err.Error()) // don't use the it variable
			return
		}

		it = gotree.New(itName + fmt.Sprintf(" (length %d)", ln))
		shift = 4

	}

	var m, s, k int

	if s = fixedSize(el.Kind()); s < 0 {

		for k = 0; k < ln; k++ {

			if shift+s > len(val) {
				it.Add(
					fmt.Sprintf("(err) unexpected end of %s at %d element",
						sch.Kind().String(), k),
				)
				gt.AddTree(it)
				return
			}

			rootTreeData(it, pack, el, val[shift:shift+s])
			shift += s

		}

	} else {

		for k = 0; k < ln; k++ {

			if shift > len(val) {
				it.Add(
					fmt.Sprintf("(err) unexpected end of %s at %d element",
						sch.Kind().String(), k),
				)
				gt.AddTree(it)
				return
			}

			if m, err = el.Size(val[shift:]); err != nil {
				it.Add(
					fmt.Sprintf("(err) invalid object size at %d (%s): %v",
						k, sch.Kind().String(), err),
				)
				gt.AddTree(it)
				return
			}

			rootTreeData(it, pack, el, val[shift:shift+m])
			shift += m

		}

	}

	gt.AddTree(it)
}

func rootTreeStruct(
	gt gotree.Tree, // :
	pack Pack, //      :
	sch Schema, //     :
	val []byte, //     :
) {

	var (
		shift int
		s     int
		err   error

		it = gotree.New(sch.String())
	)

	for _, f := range sch.Fields() {

		if shift > len(val) {
			it.Add(
				fmt.Sprintf("(err) unexpected end of encoded struct '%s' "+
					"at field '%s', schema of field: '%s'",
					sch.String(), f.Name(), f.Schema().String()),
			)
			gt.AddTree(it)
			return

		}

		if s, err = f.Schema().Size(val[shift:]); err != nil {
			it.Add("(err) " + err.Error())
			gt.AddTree(it)
			return
		}

		// TOTH (kotyarin): short 'Field: value (type T)' output

		var fit = gotree.New(f.Name() + ": ")
		rootTreeData(fit, pack, f.Schema(), val[shift:shift+s])
		it.AddTree(fit)
		shift += s

	}

	gt.AddTree(it)
}

func rootTreeRef(
	gt gotree.Tree, // :
	pack Pack, //      :
	sch Schema, //     :
	val []byte, //     :
) {

	var (
		ref Ref
		el  Schema
		err error
	)

	if el = sch.Elem(); el == nil {
		gt.Add(fmt.Sprintf("*(<ref>) err: missing schema of element: %s ", sch))
		return
	}

	if err = encoder.DeserializeRaw(val, &ref); err != nil {
		gt.Add(fmt.Sprintf("*(%s) err: %s", el.String(), err.Error()))
		return
	}

	if ref.Hash == (cipher.SHA256{}) {
		gt.Add(fmt.Sprintf("*(%s) nil", el.String()))
		return
	}

	var it = gotree.New(fmt.Sprintf("*(%s) %s", el.String(), ref.Short()))
	rootTreeHash(it, pack, el, ref.Hash)
	gt.AddTree(it)
}

func rootTreeRefs(
	gt gotree.Tree, // :
	pack Pack, //      :
	sch Schema, //     :
	val []byte, //     :
) {

	var (
		refs Refs
		el   Schema
		err  error
	)

	if el = sch.Elem(); el == nil {
		gt.Add("[]*(<refs>) err: missing schema of element")
		return
	}

	if err = encoder.DeserializeRaw(val, &refs); err != nil {
		gt.Add(fmt.Sprintf("[]*(-----) %s err: %s", el.String(), err.Error()))
		return
	}

	// initialize first

	var ln int
	if ln, err = refs.Len(pack); err != nil {
		gt.Add(
			fmt.Sprintf("[]*(%s) %s err: %s",
				el.String(), refs.Short(), err.Error()),
		)
		return
	}

	// can be blank

	if ln == 0 {
		gt.Add(fmt.Sprintf("[]*(%s) %s nil", el.String(), refs.Short()))
		return
	}

	var it = gotree.New(
		fmt.Sprintf("[]*(%s) %s length: %d", el.String(), refs.Short(), ln),
	)

	err = refs.Walk(pack, el, func(
		hash cipher.SHA256,
		depth int,
	) (
		deepper bool,
		err error,
	) {

		if depth != 0 {
			return true, nil
		}

		rootTreeHash(it, pack, el, hash)
		return true, nil
	})

	if err != nil {
		it.Add("err: " + err.Error())
	}

	gt.AddTree(it)
}
