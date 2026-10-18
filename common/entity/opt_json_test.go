// Copyright 2023-2026 The Oxia Authors
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

package entity

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type myStruct struct {
	Name    string                `json:"name"`
	Enabled OptBooleanDefaultTrue `json:"enabled"`
}

func TestOptBoolean_MarshallTrue(t *testing.T) {
	ms := &myStruct{
		Name:    "hello",
		Enabled: Bool(true),
	}
	ser, err := json.Marshal(ms)
	assert.NoError(t, err)

	assert.Equal(t, `{"name":"hello","enabled":true}`, string(ser))
}

func TestOptBoolean_MarshallFalse(t *testing.T) {
	ms := &myStruct{
		Name:    "hello",
		Enabled: Bool(false),
	}
	ser, err := json.Marshal(ms)
	assert.NoError(t, err)

	assert.Equal(t, `{"name":"hello","enabled":false}`, string(ser))
}

func TestOptBooleanDefaultTrueMarshalYAML(t *testing.T) {
	ser, err := yaml.Marshal(&myStruct{Name: "hello", Enabled: Bool(false)})
	require.NoError(t, err)
	assert.Equal(t, "name: hello\nenabled: false\n", string(ser))

	ser, err = yaml.Marshal(&myStruct{Name: "hello"})
	require.NoError(t, err)
	assert.Equal(t, "name: hello\nenabled: null\n", string(ser))
}

func TestOptBoolean_UnmarshallTrue(t *testing.T) {
	data := []byte(`{"name":"hello", "enabled":true}`)

	ms := &myStruct{}
	err := json.Unmarshal(data, ms)
	assert.NoError(t, err)

	assert.Equal(t, "hello", ms.Name)
	assert.True(t, ms.Enabled.Get())
}

func TestOptBoolean_UnmarshallFalse(t *testing.T) {
	data := []byte(`{"name":"hello", "enabled":false}`)

	ms := &myStruct{}
	err := json.Unmarshal(data, ms)
	assert.NoError(t, err)

	assert.Equal(t, "hello", ms.Name)
	assert.False(t, ms.Enabled.Get())
}

func TestOptBoolean_UnmarshallMissing(t *testing.T) {
	data := []byte(`{"name":"hello"}`)

	ms := &myStruct{}
	err := json.Unmarshal(data, ms)
	assert.NoError(t, err)

	assert.Equal(t, "hello", ms.Name)
	assert.True(t, ms.Enabled.Get())
}

func TestOptBoolean_UnmarshallNull(t *testing.T) {
	data := []byte(`{"name":"hello", "enabled":null}`)

	ms := &myStruct{}
	err := json.Unmarshal(data, ms)
	assert.NoError(t, err)

	assert.Equal(t, "hello", ms.Name)
	assert.True(t, ms.Enabled.Get())
}

func TestOptBoolean_UnmarshallError(t *testing.T) {
	ms := &myStruct{}

	err := json.Unmarshal([]byte(`{"name":"hello", "enabled":5}`), ms)
	assert.Error(t, err)

	err = json.Unmarshal([]byte(`{"name":"hello", "enabled":"invalid"}`), ms)
	assert.Error(t, err)
}

func TestOptBooleanViperHookConvertsBool(t *testing.T) {
	hook := OptBooleanViperHook()
	optType := reflect.TypeOf(OptBooleanDefaultTrue{})

	res, err := hook(reflect.TypeOf(false), optType, false)
	require.NoError(t, err)
	opt, ok := res.(OptBooleanDefaultTrue)
	require.True(t, ok)
	assert.False(t, opt.Get())

	res, err = hook(reflect.TypeOf(true), optType, true)
	require.NoError(t, err)
	opt, ok = res.(OptBooleanDefaultTrue)
	require.True(t, ok)
	assert.True(t, opt.Get())
}

func TestOptBooleanViperHookRejectsNonBool(t *testing.T) {
	res, err := OptBooleanViperHook()(reflect.TypeOf(""), reflect.TypeOf(OptBooleanDefaultTrue{}), "yes")
	assert.EqualError(t, err, "invalid boolean value: 'yes'")
	assert.Nil(t, res)
}

func TestOptBooleanViperHookPassesOtherTypes(t *testing.T) {
	res, err := OptBooleanViperHook()(reflect.TypeOf(""), reflect.TypeOf(""), "value")
	require.NoError(t, err)
	assert.Equal(t, "value", res)
}
