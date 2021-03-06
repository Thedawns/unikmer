// Copyright © 2018-2020 Wei Shen <shenwei356@gmail.com>
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package unikmer

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/shenwei356/breader"
)

// Taxonomy holds relationship of taxon in a taxonomy.
type Taxonomy struct {
	file     string
	rootNode uint32

	Nodes      map[uint32]uint32 // parent -> child
	DelNodes   map[uint32]struct{}
	MergeNodes map[uint32]uint32

	taxid2rankid map[uint32]uint8 // taxid -> rank id
	ranks        []string         // rank id -> rank
	Ranks        map[string]interface{}

	hasRanks      bool
	hasDelNodes   bool
	hasMergeNodes bool

	cacheLCA bool
	// lcaCache map[uint64]uint32 // cache of lca
	// mux      sync.Mutex
	lcaCache sync.Map

	maxTaxid uint32
}

// ErrIllegalColumnIndex means column index is 0 or negative.
var ErrIllegalColumnIndex = errors.New("unikmer: illegal column index, positive integer needed")

// ErrRankNotLoaded means you should reate load Taxonomy with NewTaxonomyWithRank before calling some methods.
var ErrRankNotLoaded = errors.New("unikmer: ranks not loaded, please call: NewTaxonomyWithRank")

// ErrTooManyRanks means number of ranks exceed limit of 255
var ErrTooManyRanks = errors.New("unikmer: number of ranks exceed limit of 255")

// NewTaxonomyFromNCBI parses Taxonomy from nodes.dmp
// from ftp://ftp.ncbi.nih.gov/pub/taxonomy/taxdump.tar.gz .
func NewTaxonomyFromNCBI(file string) (*Taxonomy, error) {
	return NewTaxonomy(file, 1, 3)
}

// NewTaxonomy loads nodes from nodes.dmp file.
func NewTaxonomy(file string, childColumn int, parentColumn int) (*Taxonomy, error) {
	if childColumn < 1 || parentColumn < 1 {
		return nil, ErrIllegalColumnIndex
	}
	minColumns := minInt(childColumn, parentColumn)

	// taxon represents a taxonomic node
	type taxon struct {
		Taxid  uint32
		Parent uint32
	}

	childColumn--
	parentColumn--
	parseFunc := func(line string) (interface{}, bool, error) {
		line = strings.TrimSpace(line)
		if line == "" {
			return nil, false, nil
		}
		items := strings.Split(line, "\t")
		if len(items) < minColumns {
			return nil, false, nil
		}
		child, e := strconv.Atoi(items[childColumn])
		if e != nil {
			return nil, false, e
		}
		parent, e := strconv.Atoi(items[parentColumn])
		if e != nil {
			return nil, false, e
		}
		return taxon{Taxid: uint32(child), Parent: uint32(parent)}, true, nil
	}

	reader, err := breader.NewBufferedReader(file, 8, 100, parseFunc)
	if err != nil {
		return nil, fmt.Errorf("unikmer: %s", err)
	}

	nodes := make(map[uint32]uint32, 1024)
	var root uint32

	var tax taxon
	var data interface{}
	var maxTaxid uint32
	for chunk := range reader.Ch {
		if chunk.Err != nil {
			return nil, fmt.Errorf("unikmer: %s", chunk.Err)
		}
		for _, data = range chunk.Data {
			tax = data.(taxon)

			nodes[tax.Taxid] = tax.Parent

			if tax.Taxid == tax.Parent {
				root = tax.Taxid
			}
			if tax.Taxid > maxTaxid {
				maxTaxid = tax.Taxid
			}
		}
	}

	return &Taxonomy{file: file, Nodes: nodes, rootNode: root, maxTaxid: maxTaxid}, nil
}

// NewTaxonomyWithRankFromNCBI parses Taxonomy from nodes.dmp
// from ftp://ftp.ncbi.nih.gov/pub/taxonomy/taxdump.tar.gz .
func NewTaxonomyWithRankFromNCBI(file string) (*Taxonomy, error) {
	return NewTaxonomyWithRank(file, 1, 3, 5)
}

// NewTaxonomyWithRank loads nodes and ranks from nodes.dmp file.
func NewTaxonomyWithRank(file string, childColumn int, parentColumn int, rankColumn int) (*Taxonomy, error) {
	if childColumn < 1 || parentColumn < 1 || rankColumn < 1 {
		return nil, ErrIllegalColumnIndex
	}
	minColumns := minInt(childColumn, parentColumn, rankColumn)

	// taxon represents a taxonomic node
	type taxon struct {
		Taxid  uint32
		Parent uint32
		Rank   string
	}

	childColumn--
	parentColumn--
	rankColumn--
	parseFunc := func(line string) (interface{}, bool, error) {
		line = strings.TrimSpace(line)
		if line == "" {
			return nil, false, nil
		}
		items := strings.Split(line, "\t")
		if len(items) < minColumns {
			return nil, false, nil
		}
		child, e := strconv.Atoi(items[childColumn])
		if e != nil {
			return nil, false, e
		}
		parent, e := strconv.Atoi(items[parentColumn])
		if e != nil {
			return nil, false, e
		}
		return taxon{Taxid: uint32(child), Parent: uint32(parent), Rank: items[rankColumn]}, true, nil
	}

	reader, err := breader.NewBufferedReader(file, 8, 100, parseFunc)
	if err != nil {
		return nil, fmt.Errorf("unikmer: %s", err)
	}

	nodes := make(map[uint32]uint32, 1024)
	var root uint32

	var tax taxon
	var data interface{}
	var maxTaxid uint32

	taxid2rankid := make(map[uint32]uint8, 1024)
	ranks := make([]string, 0, 100)
	rank2rankid := make(map[string]int, 100)
	ranksMap := make(map[string]interface{}, 100)

	var ok bool
	var rankid int
	for chunk := range reader.Ch {
		if chunk.Err != nil {
			return nil, fmt.Errorf("unikmer: %s", chunk.Err)
		}
		for _, data = range chunk.Data {
			tax = data.(taxon)

			nodes[tax.Taxid] = tax.Parent

			if tax.Taxid == tax.Parent {
				root = tax.Taxid
			}
			if tax.Taxid > maxTaxid {
				maxTaxid = tax.Taxid
			}

			if rankid, ok = rank2rankid[tax.Rank]; ok {
				taxid2rankid[tax.Taxid] = uint8(rankid)
			} else {
				ranks = append(ranks, tax.Rank)
				if len(ranks) > 255 {
					return nil, ErrTooManyRanks
				}
				rank2rankid[tax.Rank] = len(ranks) - 1
				taxid2rankid[tax.Taxid] = uint8(len(ranks) - 1)
				ranksMap[tax.Rank] = struct{}{}
			}
		}
	}

	return &Taxonomy{file: file, Nodes: nodes, rootNode: root, maxTaxid: maxTaxid,
		taxid2rankid: taxid2rankid, ranks: ranks, hasRanks: true, Ranks: ranksMap}, nil
}

// Rank returns rank of a taxid.
func (t *Taxonomy) Rank(taxid uint32) string {
	if !t.hasRanks {
		panic(ErrRankNotLoaded)
	}
	if i, ok := t.taxid2rankid[taxid]; ok {
		return t.ranks[int(i)]
	}
	return "" // taxid not found int db
}

// LoadMergedNodesFromNCBI loads merged nodes from  NCBI merged.dmp.
func (t *Taxonomy) LoadMergedNodesFromNCBI(file string) error {
	return t.LoadMergedNodes(file, 1, 3)
}

// LoadMergedNodes loads merged nodes.
func (t *Taxonomy) LoadMergedNodes(file string, oldColumn int, newColumn int) error {
	if oldColumn < 1 || newColumn < 1 {
		return ErrIllegalColumnIndex
	}

	minColumns := oldColumn
	if newColumn > minColumns {
		minColumns = newColumn
	}

	oldColumn--
	newColumn--
	parseFunc := func(line string) (interface{}, bool, error) {
		items := strings.Split(strings.TrimSpace(line), "\t")
		if len(items) < minColumns {
			return nil, false, nil
		}
		old, e := strconv.Atoi(items[oldColumn])
		if e != nil {
			return nil, false, e
		}
		new, e := strconv.Atoi(items[newColumn])
		if e != nil {
			return nil, false, e
		}
		return [2]uint32{uint32(old), uint32(new)}, true, nil
	}

	m := make(map[uint32]uint32, 1024)
	reader, err := breader.NewBufferedReader(file, 3, 50, parseFunc)
	if err != nil {
		return fmt.Errorf("unikmer: %s", err)
	}

	var p [2]uint32
	var data interface{}
	for chunk := range reader.Ch {
		if chunk.Err != nil {
			return fmt.Errorf("unikmer: %s", chunk.Err)
		}

		for _, data = range chunk.Data {
			p = data.([2]uint32)
			m[p[0]] = p[1]
		}
	}
	t.MergeNodes = m
	t.hasMergeNodes = true
	return nil
}

// LoadDeletedNodesFromNCBI loads deleted nodes from NCBI delnodes.dmp.
func (t *Taxonomy) LoadDeletedNodesFromNCBI(file string) error {
	return t.LoadDeletedNodes(file, 1)
}

// LoadDeletedNodes loads deleted nodes.
func (t *Taxonomy) LoadDeletedNodes(file string, column int) error {
	if column < 1 {
		return ErrIllegalColumnIndex
	}

	parseFunc := func(line string) (interface{}, bool, error) {
		items := strings.Split(strings.TrimSpace(line), "\t")
		if len(items) < column {
			return nil, false, nil
		}
		id, e := strconv.Atoi(items[column-1])
		if e != nil {
			return nil, false, e
		}
		return uint32(id), true, nil
	}

	m := make(map[uint32]struct{}, 1024)
	reader, err := breader.NewBufferedReader(file, 3, 50, parseFunc)
	if err != nil {
		return fmt.Errorf("unikmer: %s", err)
	}

	var taxid uint32
	var data interface{}
	for chunk := range reader.Ch {
		if chunk.Err != nil {
			return fmt.Errorf("unikmer: %s", chunk.Err)
		}

		for _, data = range chunk.Data {
			taxid = data.(uint32)
			m[taxid] = struct{}{}
		}
	}
	t.DelNodes = m
	t.hasDelNodes = true
	return nil
}

// MaxTaxid returns maximum taxid
func (t *Taxonomy) MaxTaxid() uint32 {
	return t.maxTaxid
}

// CacheLCA tells to cache every LCA query result
func (t *Taxonomy) CacheLCA() {
	t.cacheLCA = true
	// if t.lcaCache == nil {
	// 	t.lcaCache = make(map[uint64]uint32, 1024)
	// }
}

// LCA returns the Lowest Common Ancestor of two nodes, 0 for unknown taxid.
func (t *Taxonomy) LCA(a uint32, b uint32) uint32 {
	if a == 0 || b == 0 {
		return 0
	}
	if a == b {
		return a
	}

	// check cache
	var ok bool

	var query uint64
	var tmp interface{}
	if t.cacheLCA {
		query = pack2uint32(a, b)

		tmp, ok = t.lcaCache.Load(query)
		if ok {
			return tmp.(uint32)
		}
	}

	lineA := make([]uint32, 0, 16)
	mA := make(map[uint32]struct{}, 16)

	var child, parent, newTaxid uint32
	var flag bool

	child = a
	for {
		parent, ok = t.Nodes[child]
		if !ok {
			flag = false
			if t.hasMergeNodes { // merged?
				if newTaxid, ok = t.MergeNodes[child]; ok { // merged
					child = newTaxid // update child

					parent, ok = t.Nodes[child]
					if ok {
						flag = true
					}
				}
			}

			if !flag {
				if t.cacheLCA {
					t.lcaCache.Store(query, uint32(0))
				}
				return 0
			}
		}
		if parent == child { // root
			lineA = append(lineA, parent)
			mA[parent] = struct{}{}
			break
		}
		if parent == b { // b is ancestor of a
			if t.cacheLCA {
				t.lcaCache.Store(query, b)
			}
			return b
		}
		lineA = append(lineA, parent)
		mA[parent] = struct{}{}

		child = parent
	}

	child = b
	for {
		parent, ok = t.Nodes[child]
		if !ok {
			flag = false
			if t.hasMergeNodes { // merged?
				if newTaxid, ok = t.MergeNodes[child]; ok { // merged
					child = newTaxid // update child

					parent, ok = t.Nodes[child]
					if ok {
						flag = true
					}
				}
			}

			if !flag {
				if t.cacheLCA {
					t.lcaCache.Store(query, uint32(0))
				}
				return 0
			}
		}

		if parent == child { // root
			break
		}
		if parent == a { // a is ancestor of b
			if t.cacheLCA {
				t.lcaCache.Store(query, a)
			}
			return a
		}
		if _, ok = mA[parent]; ok {
			if t.cacheLCA {
				t.lcaCache.Store(query, parent)
			}
			return parent
		}

		child = parent
	}
	return t.rootNode
}

func pack2uint32(a uint32, b uint32) uint64 {
	if a < b {
		return (uint64(a) << 32) | uint64(b)
	}
	return (uint64(b) << 32) | uint64(a)
}

func minInt(a int, vals ...int) int {
	min := a
	for _, v := range vals {
		if v < min {
			min = v
		}
	}
	return min
}
