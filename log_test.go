// Copyright 2020
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package gologs

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBeeLoggerDelLogger(t *testing.T) {
	prefix := "My-Cus"
	l := GetLogger(prefix)
	assert.NotNil(t, l)
	l.Print("hello")

	GetLogger().Print("hello")
	SetPrefix("aaa")
	Info("hello")
}
