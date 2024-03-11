/*
 * Copyright 2019 Dgraph Labs, Inc. and Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package pending

import (
	"bytes"
)

// mergeIterator merges multiple iterators.
// NOTE: mergeIterator owns the array of iterators and is responsible for closing them.
type mergeIterator struct {
	left  node
	right node
	small *node

	curKey  []byte
	reverse bool
}

type node struct {
	valid bool
	key   []byte
	iter  iterator
}

func (n *node) setIterator(iter iterator) {
	n.iter = iter
}

func (n *node) setKey() {
	n.valid = n.iter.Valid()
	if n.valid {
		n.key = n.iter.Key()
	}
}

func (n *node) next() {
	n.iter.Next()
	n.setKey()
}

func (n *node) rewind() {
	n.iter.Rewind()
	n.setKey()
}

func (n *node) seek(key []byte) {
	n.iter.Seek(key)
	n.setKey()
}

func (mi *mergeIterator) fix() {
	if !mi.bigger().valid {
		return
	}
	if !mi.small.valid {
		mi.swapSmall()
		return
	}
	cmp := bytes.Compare(mi.small.key, mi.bigger().key)
	switch {
	case cmp == 0: // Both the keys are equal.
		// In case of same keys, move the right iterator ahead.
		mi.right.next()
		if &mi.right == mi.small {
			mi.swapSmall()
		}
		return
	case cmp < 0: // Small is less than bigger().
		if mi.reverse {
			mi.swapSmall()
		} else {
			// we don't need to do anything. Small already points to the smallest.
		}
		return
	default: // bigger() is less than small.
		if mi.reverse {
			// Do nothing since we're iterating in reverse. Small currently points to
			// the bigger key and that's okay in reverse iteration.
		} else {
			mi.swapSmall()
		}
		return
	}
}

func (mi *mergeIterator) bigger() *node {
	if mi.small == &mi.left {
		return &mi.right
	}
	return &mi.left
}

func (mi *mergeIterator) swapSmall() {
	if mi.small == &mi.left {
		mi.small = &mi.right
		return
	}
	if mi.small == &mi.right {
		mi.small = &mi.left
		return
	}
}

// Next returns the next element. If it is the same as the current key, ignore it.
func (mi *mergeIterator) Next() {
	for mi.Valid() {
		if !bytes.Equal(mi.small.key, mi.curKey) && !mi.small.iter.Item().IsDeletedOrExpired() {
			break
		}
		mi.small.next()
		mi.fix()
	}
	mi.setCurrent()
}

func (mi *mergeIterator) setCurrent() {
	mi.curKey = append(mi.curKey[:0], mi.small.key...)
}

// Rewind seeks to first element (or last element for reverse iterator).
func (mi *mergeIterator) Rewind() {
	mi.left.rewind()
	mi.right.rewind()
	mi.fix()
	mi.setCurrent()
	for mi.Valid() {
		if mi.Item().IsDeletedOrExpired() {
			mi.Next()
		}
		break
	}
}

// Seek brings us to element with key >= given key.
func (mi *mergeIterator) Seek(key []byte) {
	mi.left.seek(key)
	mi.right.seek(key)
	mi.fix()
	mi.setCurrent()
}

// Valid returns whether the mergeIterator is at a valid element.
func (mi *mergeIterator) Valid() bool {
	return mi.small.valid
}

// Key returns the key associated with the current iterator.
func (mi *mergeIterator) Key() []byte {
	return mi.small.key
}

// Item returns the value associated with the iterator.
func (mi *mergeIterator) Item() Item {
	return mi.small.iter.Item()
}

// Close implements y.Iterator.
func (mi *mergeIterator) Close() {
	mi.left.iter.Close()
	mi.right.iter.Close()
}

// NewMergeIterator creates a merge iterator.
func newMergeIterator(txi *txIterator, pending iterator, reverse bool) Iterator {
	mi := &mergeIterator{
		reverse: reverse,
	}
	mi.left.setIterator(pending)
	mi.right.setIterator(txi)
	// Assign left iterator randomly. This will be fixed when user calls rewind/seek.
	mi.small = &mi.left
	return mi
}
