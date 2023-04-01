package rule

import "sync"

type TableRules struct {
	mu                 sync.RWMutex
	dbRender, tbRender func(int) string
	idx                sync.Map // map[int][]int
}
