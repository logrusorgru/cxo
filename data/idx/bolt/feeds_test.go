package idxdb

import (
	"testing"

	"github.com/skycoin/cxo/data/tests"
)

func TestFeeds_Add(t *testing.T) {
	runTestCase(t, tests.FeedsAdd)
}

func TestFeeds_Del(t *testing.T) {
	runTestCase(t, tests.FeedsDel)
}

func TestFeeds_Iterate(t *testing.T) {
	runTestCase(t, tests.FeedsIterate)
}

func TestFeeds_Has(t *testing.T) {
	runTestCase(t, tests.FeedsHas)
}

func TestFeeds_Heads(t *testing.T) {
	runTestCase(t, tests.FeedsHeads)
}
