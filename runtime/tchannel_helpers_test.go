// Copyright (c) 2024 Uber Technologies, Inc.
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

package zanzibar

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"

	"github.com/uber/tchannel-go/testutils/testreader"
	"go.uber.org/thriftrw/plugin/api"
	"go.uber.org/thriftrw/protocol/binary"
	"go.uber.org/thriftrw/wire"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEnsureEmptySuccess(t *testing.T) {
	reader := bytes.NewReader(nil)
	err := EnsureEmpty(reader, "success")
	require.NoError(t, err, "ensureEmpty should succeed with empty reader")
}

func TestEnsureEmptyHasBytes(t *testing.T) {
	reader := bytes.NewReader([]byte{1, 2, 3})
	err := EnsureEmpty(reader, "T")
	require.Error(t, err, "ensureEmpty should fail when there's bytes")
	assert.Equal(t, err.Error(), "found unexpected bytes after T, found (upto 128 bytes): 010203")
}

func TestEnsureEmptyError(t *testing.T) {
	control, reader := testreader.ChunkReader()
	control <- nil
	close(control)

	err := EnsureEmpty(reader, "has bytes")
	require.Error(t, err, "ensureEmpty should fail when there's an error")
	assert.Equal(t, testreader.ErrUser, err, "Unexpected error")
}

func TestBuffers(t *testing.T) {
	var wg sync.WaitGroup
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func() {
			for i := 0; i < 100; i++ {
				buf := GetBuffer()
				assert.Zero(t, buf.Len(), "Expected truncated buffer")

				bytesOfNoise := make([]byte, rand.Intn(5000))
				_, err := rand.Read(bytesOfNoise)
				assert.NoError(t, err, "Unexpected error from rand.Read")
				_, err = buf.Write(bytesOfNoise)
				assert.NoError(t, err, "Unexpected error from buffer.Write")

				assert.Equal(t, buf.Len(), len(bytesOfNoise), "Expected same buffer size")

				PutBuffer(buf)
			}
			wg.Done()
		}()
	}
	wg.Wait()
}

// fromWireOnly hides Decode, so ReadStruct takes the FromWire path.
type fromWireOnly struct{ v *api.GenerateServiceRequest }

func (f *fromWireOnly) ToWire() (wire.Value, error) { return f.v.ToWire() }
func (f *fromWireOnly) FromWire(w wire.Value) error { return f.v.FromWire(w) }

var _ streamDecoder = (*api.GenerateServiceRequest)(nil)

func testServiceRequest() *api.GenerateServiceRequest {
	str, i32 := api.SimpleTypeString, api.SimpleTypeInt32
	parent := api.ServiceID(1)
	req := &api.GenerateServiceRequest{
		RootServices: []api.ServiceID{1, 2},
		Services:     map[api.ServiceID]*api.Service{},
		Modules: map[api.ModuleID]*api.Module{
			1: {ImportPath: "github.com/uber/zanzibar/example", Directory: "example", ThriftFilePath: "example.thrift"},
		},
		PackagePrefix: "github.com/uber/zanzibar",
		ThriftRoot:    "/idl",
	}
	for id := api.ServiceID(1); id <= 2; id++ {
		svc := &api.Service{
			Name:        fmt.Sprintf("Service%d", id),
			ThriftName:  fmt.Sprintf("service%d", id),
			ModuleID:    1,
			Annotations: map[string]string{"owner": "zanzibar"},
		}
		if id == 2 {
			svc.ParentID = &parent
		}
		for f := 0; f < 4; f++ {
			svc.Functions = append(svc.Functions, &api.Function{
				Name:       fmt.Sprintf("Method%d", f),
				ThriftName: fmt.Sprintf("method%d", f),
				Arguments: []*api.Argument{
					{Name: "id", Type: &api.Type{SimpleType: &i32}},
					{Name: "tags", Type: &api.Type{MapType: &api.TypePair{
						Left:  &api.Type{SimpleType: &str},
						Right: &api.Type{SliceType: &api.Type{SimpleType: &str}},
					}}},
				},
				ReturnType: &api.Type{SliceType: &api.Type{ReferenceType: &api.TypeReference{
					Name:       "Item",
					ImportPath: "github.com/uber/zanzibar/example",
				}}},
			})
		}
		req.Services[id] = svc
	}
	return req
}

func encodeStruct(t testing.TB, s RWTStruct) []byte {
	w, err := s.ToWire()
	require.NoError(t, err)
	var buf bytes.Buffer
	require.NoError(t, binary.Default.Encode(w, &buf))
	return buf.Bytes()
}

func TestReadStruct(t *testing.T) {
	want := testServiceRequest()
	data := encodeStruct(t, want)

	_, ok := RWTStruct(&fromWireOnly{}).(streamDecoder)
	require.False(t, ok, "fromWireOnly must not implement streamDecoder")

	tests := []struct {
		name      string
		newReader func() io.Reader
	}{
		{"io.Reader", func() io.Reader { return struct{ io.Reader }{bytes.NewReader(data)} }},
		{"io.ReaderAt", func() io.Reader { return bytes.NewReader(data) }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := &api.GenerateServiceRequest{}
			require.NoError(t, ReadStruct(tt.newReader(), got))
			assert.Equal(t, want, got)

			fallback := &fromWireOnly{v: &api.GenerateServiceRequest{}}
			require.NoError(t, ReadStruct(tt.newReader(), fallback))
			assert.Equal(t, want, fallback.v)
		})
	}
}

func TestReadStructTruncated(t *testing.T) {
	data := encodeStruct(t, testServiceRequest())
	for _, n := range []int{0, 1, len(data) / 2, len(data) - 1} {
		err := ReadStruct(bytes.NewReader(data[:n]), &api.GenerateServiceRequest{})
		assert.Error(t, err, "Decode path, %d bytes", n)
		err = ReadStruct(bytes.NewReader(data[:n]), &fromWireOnly{v: &api.GenerateServiceRequest{}})
		assert.Error(t, err, "FromWire path, %d bytes", n)
	}
}

func BenchmarkReadStruct(b *testing.B) {
	data := encodeStruct(b, testServiceRequest())
	benchmarks := []struct {
		name      string
		newStruct func() RWTStruct
	}{
		{"Decode", func() RWTStruct { return &api.GenerateServiceRequest{} }},
		{"FromWire", func() RWTStruct { return &fromWireOnly{v: &api.GenerateServiceRequest{}} }},
	}
	for _, bb := range benchmarks {
		b.Run(bb.name, func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(int64(len(data)))
			for b.Loop() {
				if err := ReadStruct(struct{ io.Reader }{bytes.NewReader(data)}, bb.newStruct()); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
