// Copyright 2024 Linka Cloud  All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package badgerd

import (
	"bytes"
)

type iterator struct {
	Iterator
}

func (i *iterator) Next() {
	i.Iterator.Next()
	k := i.Iterator.Key()
	for bytes.HasPrefix(k, internalPrefix) && i.Iterator.Valid() {
		i.Iterator.Next()
		k = i.Iterator.Key()
	}
}
