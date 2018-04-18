package registry

import (
	"fmt"
	"testing"

	"github.com/skycoin/skycoin/src/cipher"
)

// the method panics on failure
func dynamicByValue(pack Pack, val interface{}) (dr Dynamic) {

	var (
		schemaName = pack.Registry().Types().Inverse[typeOf(val)]
		sch, err   = pack.Registry().SchemaByName(schemaName)
	)

	if err != nil {
		panic(err)
	}

	dr.Schema = sch.Reference()
	dr.Hash, err = addToPack(pack, val)

	if err != nil {
		panic(err)
	}

	return
}

func TestRoot_Tree(t *testing.T) {
	// (*Root) Tree(pack Pack) (tree string)

	// ----------------------------------------------------------------------
	// blank
	// ----------------------------------------------------------------------

	var (
		pack = testPackReg(nil) // no Registry
		r    = new(Root)

		tree string
	)

	r.Nonce = 9210
	r.Seq = 4
	r.Hash = cipher.SumSHA256([]byte("any"))
	r.Pub, _ = cipher.GenerateKeyPair()

	tree = r.Tree(pack)
	t.Log(tree)

	const blankWantFormat = `(root) %s %s (reg: 0000000)
└── (blank)
`

	var blankWant = fmt.Sprintf(blankWantFormat,
		r.Hash.Hex()[:7], r.Short())

	if tree != blankWant {
		t.Errorf("unexpected tree:\ngot: %s\nwant: %s", tree, blankWant)
	}

	// ----------------------------------------------------------------------
	// error: no registry
	// ----------------------------------------------------------------------

	r.Refs = []Dynamic{{}, {}} // for the test

	tree = r.Tree(pack)
	t.Log(tree)

	const errNoRegistryWantFormat = `(root) %s %s (reg: 0000000)
└── (err) missing registry in Pack
`
	var errNoRegistryWant = fmt.Sprintf(errNoRegistryWantFormat,
		r.Hash.Hex()[:7], r.Short())

	if tree != errNoRegistryWant {
		t.Errorf("unexpected tree:\ngot: %s\nwant: %s", tree, errNoRegistryWant)
	}

	// ----------------------------------------------------------------------
	// empty
	// ----------------------------------------------------------------------

	r.Refs = nil // clear
	var reg = testRegistry()
	pack = testPackReg(reg) // with the reg
	r.Reg = reg.Reference() // use the reg

	tree = r.Tree(pack)
	t.Log(tree)

	const emptyWantFormat = `(root) %s %s (reg: %s)
└── (empty)
`
	var emptyWant = fmt.Sprintf(emptyWantFormat,
		r.Hash.Hex()[:7], r.Short(), reg.Reference().Short())

	if tree != emptyWant {
		t.Errorf("unexpected tree:\ngot: %s\nwant: %s", tree, emptyWant)
	}

	// ----------------------------------------------------------------------
	// (nil, nil) dynamic (without schema)
	// ----------------------------------------------------------------------

	r.Refs = []Dynamic{{}}

	tree = r.Tree(pack)
	t.Log(tree)

	const nilNilDynamicWantFormat = `(root) %s %s (reg: %s)
└── *(dynamic) nil
`
	var nilNilDynamicWant = fmt.Sprintf(nilNilDynamicWantFormat,
		r.Hash.Hex()[:7], r.Short(), reg.Reference().Short())

	if tree != nilNilDynamicWant {
		t.Errorf("unexpected tree:\ngot: %s\nwant: %s", tree, nilNilDynamicWant)
	}

	// ----------------------------------------------------------------------
	// (User, nil) dynamic (with schema of "test.User")
	// ----------------------------------------------------------------------

	var sch, err = reg.SchemaByName("test.User")

	if err != nil {
		t.Fatal(err)
	}

	// nil of "test.User"
	r.Refs = []Dynamic{{Schema: sch.Reference()}}

	tree = r.Tree(pack)
	t.Log(tree)

	const nilUserDynamicWantFormat = `(root) %s %s (reg: %s)
└── *(dynamic) nil (type test.User)
`
	var nilUserDynamicWant = fmt.Sprintf(nilUserDynamicWantFormat,
		r.Hash.Hex()[:7], r.Short(), reg.Reference().Short())

	if tree != nilUserDynamicWant {
		t.Errorf("unexpected tree:\ngot: %s\nwant: %s",
			tree, nilUserDynamicWant)
	}

	// ----------------------------------------------------------------------
	// Alice dynamic (TestUser)
	// ----------------------------------------------------------------------

	var (
		alice        = &TestUser{Name: "Alice", Age: 19}
		aliceDynamic = dynamicByValue(pack, alice)
	)

	r.Refs = []Dynamic{aliceDynamic} // Alice

	tree = r.Tree(pack)
	t.Log(tree)

	const aliceDynamicWantFormat = `(root) %s %s (reg: %s)
└── *(dynamic) %s
    └── test.User
        ├── Name: 
        │   └── %s (type string)
        └── Age: 
            └── %d (type uint32)
`
	var aliceDynamicWant = fmt.Sprintf(aliceDynamicWantFormat,
		r.Hash.Hex()[:7], r.Short(), reg.Reference().Short(),
		aliceDynamic.Short(),
		alice.Name,
		alice.Age,
	)

	if tree != aliceDynamicWant {
		t.Errorf("unexpected tree:\ngot: %s\nwant: %s", tree, aliceDynamicWant)
	}

	// ----------------------------------------------------------------------
	// + empty group (TestGroup) (no links)
	// ----------------------------------------------------------------------

	var (
		group        = &TestGroup{Name: "the Group"}
		groupDynamic = dynamicByValue(pack, group)
	)

	r.Refs = append(r.Refs, groupDynamic)

	tree = r.Tree(pack)
	t.Log(tree)

	const emptyGroupDynamicWantFormat = `(root) %s %s (reg: %s)
├── *(dynamic) %s
│   └── test.User
│       ├── Name: 
│       │   └── %s (type string)
│       └── Age: 
│           └── %d (type uint32)
└── *(dynamic) %s
    └── test.Group
        ├── Name: 
        │   └── %s (type string)
        ├── Members: 
        │   └── []*(test.User) 0000000 nil
        ├── Curator: 
        │   └── *(test.User) nil
        └── Developer: 
            └── *(dynamic) nil
`
	var emptyGroupDynamicWant = fmt.Sprintf(emptyGroupDynamicWantFormat,
		r.Hash.Hex()[:7], r.Short(), reg.Reference().Short(),
		aliceDynamic.Short(),
		alice.Name,
		alice.Age,
		groupDynamic.Short(),
		group.Name,
	)

	if tree != emptyGroupDynamicWant {
		t.Errorf("unexpected tree:\ngot: %s\nwant: %s", tree,
			emptyGroupDynamicWant)
	}

	// ----------------------------------------------------------------------
	// + Eva (curetor of the group)
	// ----------------------------------------------------------------------

	var (
		eva     = &TestUser{Name: "Eva", Age: 21}
		evaHash cipher.SHA256
	)

	if evaHash, err = addToPack(pack, eva); err != nil {
		t.Fatal(err)
	}

	// update the group
	group.Curator.Hash = evaHash
	groupDynamic = dynamicByValue(pack, group)

	// update the Refs (the group has been chagned)
	r.Refs[1] = groupDynamic

	tree = r.Tree(pack)
	t.Log(tree)

	const groupEvaDynamicWantFormat = `(root) %s %s (reg: %s)
├── *(dynamic) %s
│   └── test.User
│       ├── Name: 
│       │   └── %s (type string)
│       └── Age: 
│           └── %d (type uint32)
└── *(dynamic) %s
    └── test.Group
        ├── Name: 
        │   └── %s (type string)
        ├── Members: 
        │   └── []*(test.User) 0000000 nil
        ├── Curator: 
        │   └── *(test.User) %s
        │       └── test.User
        │           ├── Name: 
        │           │   └── %s (type string)
        │           └── Age: 
        │               └── %d (type uint32)
        └── Developer: 
            └── *(dynamic) nil
`
	var groupEvaDynamicWant = fmt.Sprintf(groupEvaDynamicWantFormat,
		r.Hash.Hex()[:7], r.Short(), reg.Reference().Short(),
		aliceDynamic.Short(),
		alice.Name,
		alice.Age,
		groupDynamic.Short(),
		group.Name,
		evaHash.Hex()[:7],
		eva.Name,
		eva.Age,
	)

	if tree != groupEvaDynamicWant {
		t.Errorf("unexpected tree:\ngot: %s\nwant: %s", tree,
			groupEvaDynamicWant)
	}

	// ----------------------------------------------------------------------
	// + members
	// ----------------------------------------------------------------------

	var (
		members = []*TestUser{
			&TestUser{Name: "Emma", Age: 23},
			&TestUser{Name: "Mia", Age: 17},
			&TestUser{Name: "Ellie", Age: 15},
			&TestUser{Name: "Lily", Age: 25},
		}
		membersHashes []cipher.SHA256
	)

	membersHashes = make([]cipher.SHA256, 0, len(members))

	for _, mmbr := range members {
		var hash cipher.SHA256
		if hash, err = addToPack(pack, mmbr); err != nil {
			t.Fatal(err)
		}
		membersHashes = append(membersHashes, hash)
	}

	// update the group
	if err = group.Members.AppendHashes(pack, membersHashes...); err != nil {
		t.Fatal(err)
	}

	groupDynamic = dynamicByValue(pack, group)

	// update the Refs (the group has been chagned)
	r.Refs[1] = groupDynamic

	tree = r.Tree(pack)
	t.Log(tree)

	const groupMembersDynamicWantFormat = `(root) %s %s (reg: %s)
├── *(dynamic) %s
│   └── test.User
│       ├── Name: 
│       │   └── %s (type string)
│       └── Age: 
│           └── %d (type uint32)
└── *(dynamic) %s
    └── test.Group
        ├── Name: 
        │   └── %s (type string)
        ├── Members: 
        │   └── []*(test.User) %s length: %d
        │       ├── test.User
        │       │   ├── Name: 
        │       │   │   └── %s (type string)
        │       │   └── Age: 
        │       │       └── %d (type uint32)
        │       ├── test.User
        │       │   ├── Name: 
        │       │   │   └── %s (type string)
        │       │   └── Age: 
        │       │       └── %d (type uint32)
        │       ├── test.User
        │       │   ├── Name: 
        │       │   │   └── %s (type string)
        │       │   └── Age: 
        │       │       └── %d (type uint32)
        │       └── test.User
        │           ├── Name: 
        │           │   └── %s (type string)
        │           └── Age: 
        │               └── %d (type uint32)
        ├── Curator: 
        │   └── *(test.User) %s
        │       └── test.User
        │           ├── Name: 
        │           │   └── %s (type string)
        │           └── Age: 
        │               └── %d (type uint32)
        └── Developer: 
            └── *(dynamic) nil
`

	var groupMembersDynamicWant = fmt.Sprintf(groupMembersDynamicWantFormat,
		r.Hash.Hex()[:7], r.Short(), reg.Reference().Short(),
		// alice
		aliceDynamic.Short(),
		alice.Name,
		alice.Age,
		// group
		groupDynamic.Short(),
		group.Name,
		//  members
		group.Members.Short(), len(members),
		//   emma
		members[0].Name, members[0].Age,
		//   mia
		members[1].Name, members[1].Age,
		//   ellie
		members[2].Name, members[2].Age,
		//   lily
		members[3].Name, members[3].Age,
		//  curator
		evaHash.Hex()[:7],
		eva.Name,
		eva.Age,
	)

	if tree != groupMembersDynamicWant {
		t.Errorf("unexpected tree:\ngot: %s\nwant: %s", tree,
			groupMembersDynamicWant)
	}

	// ----------------------------------------------------------------------
	// + developer
	// ----------------------------------------------------------------------

	var developer = &TestUser{Name: "Peppy", Age: 18}

	// sch points to "test.User" schema
	group.Developer.Schema = sch.Reference()
	if group.Developer.Hash, err = addToPack(pack, developer); err != nil {
		t.Fatal(err)
	}

	// update groupDynamic
	groupDynamic = dynamicByValue(pack, group)
	r.Refs[1] = groupDynamic

	tree = r.Tree(pack)
	t.Log(tree)

	const groupDeveloperDynamicWantFormat = `(root) %s %s (reg: %s)
├── *(dynamic) %s
│   └── test.User
│       ├── Name: 
│       │   └── %s (type string)
│       └── Age: 
│           └── %d (type uint32)
└── *(dynamic) %s
    └── test.Group
        ├── Name: 
        │   └── %s (type string)
        ├── Members: 
        │   └── []*(test.User) %s length: %d
        │       ├── test.User
        │       │   ├── Name: 
        │       │   │   └── %s (type string)
        │       │   └── Age: 
        │       │       └── %d (type uint32)
        │       ├── test.User
        │       │   ├── Name: 
        │       │   │   └── %s (type string)
        │       │   └── Age: 
        │       │       └── %d (type uint32)
        │       ├── test.User
        │       │   ├── Name: 
        │       │   │   └── %s (type string)
        │       │   └── Age: 
        │       │       └── %d (type uint32)
        │       └── test.User
        │           ├── Name: 
        │           │   └── %s (type string)
        │           └── Age: 
        │               └── %d (type uint32)
        ├── Curator: 
        │   └── *(test.User) %s
        │       └── test.User
        │           ├── Name: 
        │           │   └── %s (type string)
        │           └── Age: 
        │               └── %d (type uint32)
        └── Developer: 
            └── *(dynamic) %s
                └── test.User
                    ├── Name: 
                    │   └── %s (type string)
                    └── Age: 
                        └── %d (type uint32)
`

	var groupDeveloperDynamicWant = fmt.Sprintf(groupDeveloperDynamicWantFormat,
		r.Hash.Hex()[:7], r.Short(), reg.Reference().Short(),
		// alice
		aliceDynamic.Short(),
		alice.Name,
		alice.Age,
		// group
		groupDynamic.Short(),
		group.Name,
		//  members
		group.Members.Short(), len(members),
		//   emma
		members[0].Name, members[0].Age,
		//   mia
		members[1].Name, members[1].Age,
		//   ellie
		members[2].Name, members[2].Age,
		//   lily
		members[3].Name, members[3].Age,
		//  curator
		evaHash.Hex()[:7],
		eva.Name,
		eva.Age,
		//  developer
		group.Developer.Short(),
		developer.Name,
		developer.Age,
	)

	if tree != groupDeveloperDynamicWant {
		t.Errorf("unexpected tree:\ngot: %s\nwant: %s", tree,
			groupDeveloperDynamicWant)
	}

}
