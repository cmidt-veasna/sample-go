package data

import (
	"fmt"
	"io"
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// StreamData is a utility instance to provide streaming random data on the fly
// implement `io.ReadCloser` to support benchmark testing for large data
// including latency simulation.
type StreamData struct {
	maxSize int            // A maximum size of data to be generated
	bufGen  int            // size of generated source
	latency time.Duration  // An estimated latency on stream data
	vary    time.Duration  // A duration where latency can be vary
	writer  *io.PipeWriter // source data will be writter to pipe writer
	reader  *io.PipeReader // when io.Reader being we read from pipe reader
}

// newStreamData create an instance of `StreamData` represent virtual `io.Reader` which
// providing a generated source streaming base on maximum size `maxSize`.
// The `maxSize` must a be non negative integer and greater than zero. When both `estimatedLatency`
// and `vary` duration is zero, the simulation latency will not occurred when reading the data
// whereas if only the `very` duration is zero then the latency can be occorred bewteen 0 (no latency)
// to maximum latency `estimatedLatency`
func newStreamData(maxSize, bufGenSize int, estimatedLatency, vary time.Duration) *StreamData {
	if maxSize <= 0 {
		panic("stream data: maxSize must be non negative and greater than zero")
	} else if bufGenSize < 0 {
		panic("stream data: bufGenSize must be non negative and greater than zero")
	} else if estimatedLatency < 0 || vary < 0 {
		panic("stream data: estimatedLatency or vary must be greater or equal zero")
	}

	reader, writer := io.Pipe()
	sd := &StreamData{
		maxSize: maxSize,
		bufGen:  bufGenSize,
		reader:  reader,
		writer:  writer,
		latency: estimatedLatency,
		vary:    vary,
	}
	go sd.generateSource()
	return sd
}

// generateSource create goroutine to generate random data for reading
func (sd *StreamData) generateSource() {
	var latency func()
	if sd.latency > 0 && sd.vary > 0 {
		latency = func() {
			time.Sleep(sd.latency + time.Duration(rand.Int63n(int64(sd.vary))))
		}
	} else if sd.latency > 0 {
		latency = func() {
			time.Sleep(time.Duration(rand.Int63n(int64(sd.latency))))
		}
	} else {
		// no latency simulation, so just do nothing
		latency = func() {}
	}

	totalGen := 0
	buf := make([]byte, sd.bufGen)
	for remain := sd.maxSize - totalGen; remain > 0; remain = sd.maxSize - totalGen {
		latency()

		end := sd.bufGen
		if remain < sd.bufGen {
			// we only need `remain` random data to complete maximum size
			end = remain
		}

		nread, err := rand.Read(buf[0:end])
		if err != nil {
			sd.writer.CloseWithError(fmt.Errorf("stream data: unable to generate random data %w", err))
			return
		}

		start := 0
		for {
			n, err := sd.writer.Write(buf[start:nread])
			if err == io.ErrClosedPipe {
				// pipe has been close by Close method
				return
			}
			if err != nil {
				sd.writer.CloseWithError(fmt.Errorf("stream data: unable to write generated data %w", err))
				return
			} else if start+n == nread {
				break
			}
			start += n
		}
		totalGen += nread
	}
	sd.writer.Close()
}

// Close signal to the source generator routine to terminate it's job
// and subsequence call to Read will return `ErrStreamClose`.
func (sd *StreamData) Close() (err error) {
	sd.writer.Close()
	sd.reader.Close()
	return nil
}

// Read read data from streaming source it return err if stream is closed
// or any error occurred during generated source. It will return `io.EOF`
// stream read maximum limit generate source.
func (sd *StreamData) Read(buf []byte) (n int, err error) {
	return sd.reader.Read(buf)
}

type streamDataTestCase struct {
	maxSize    int
	minGenTime time.Duration
	maxGenTime time.Duration
	latency    time.Duration
	vary       time.Duration
	bufSize    int
}

// executionTime constant duration which given an estimate of a maximum execution time.
// Note: The execution duration usually vary depend on the hardware that execute this test.
const executionTime = 150 * time.Millisecond

var streamDataCases = []*streamDataTestCase{
	{ // case 1
		maxSize: 100,  // 100b
		bufSize: 1024, // 1mb
	},
	{ // case 2
		maxSize: 100 * 1024, // 100mb
		bufSize: 1024,       // 1mb
	},
	{ // case 3
		maxSize: 100 * 1024 * 1024, // 100gb
		bufSize: 1024,              // 1mb
	},
	{ // case 4
		maxSize: 1000 * 1024 * 1024, // 1000gb
		bufSize: 400 * 1024,         // 400mb
	},
	{ // case 5
		maxSize:    1024 * 1024, // 1gb
		bufSize:    100 * 1024,  // 100mb
		latency:    50 * time.Millisecond,
		vary:       10 * time.Millisecond,
		minGenTime: time.Duration(((1024*1024)/(100*1024))*40) * time.Millisecond,
		maxGenTime: time.Duration(((1024*1024)/(100*1024))*60)*time.Millisecond + executionTime,
	},
	{ // case 6
		maxSize:    1024 * 1024 * 10, // 10gb
		bufSize:    100 * 1024,       // 100mb
		latency:    80 * time.Millisecond,
		maxGenTime: time.Duration(((1024*1024)/(100*1024))*80)*time.Millisecond + executionTime,
	},
}

func TestStreamData(t *testing.T) {
	buf := make([]byte, 1024)
	for i, tc := range streamDataCases {
		t.Logf("TestStreamData case #%d", i+1)
		// record starting time
		stime := time.Now()
		stream := newStreamData(tc.maxSize, tc.bufSize, tc.latency, tc.vary)
		// we don't really need to know what kind of underlying data that
		// stream generated. We only need to make sure that the byte generate
		// is match with maximum size.
		totalSize := 0
		for {
			n, err := stream.Read(buf)
			if err == io.EOF {
				break
			}
			assert.NoError(t, err)
			totalSize += n
		}
		assert.Equal(t, tc.maxSize, totalSize)
		if tc.minGenTime > 0 && tc.maxGenTime > 0 {
			total := time.Since(stime)
			assert.LessOrEqual(t, tc.minGenTime, total)
			assert.GreaterOrEqual(t, tc.maxGenTime, total)
		}
		stream.Close()
	}
}

var (
	streamA = &streamDataTestCase{
		maxSize: 1024 * 1024, // 1gb
		bufSize: 400 * 1024,  // 400mb
	}
	streamB = &streamDataTestCase{
		maxSize: 10 * 1024 * 1024, // 10gb
		bufSize: 800 * 1024,       // 600mb
	}
)

// readStreamData read data from stream and discard it right away until reach io.EOF
func readStreamData(b *testing.B, stream *StreamData, bufSize int) {
	buf := make([]byte, bufSize)
	for {
		_, err := stream.Read(buf)
		if err == io.EOF {
			break
		}
		require.NoError(b, err)
	}
}

func BenchmarkStreamData1(b *testing.B) {
	for i := 0; i < b.N; i++ {
		stream := newStreamData(streamA.maxSize, streamA.bufSize, streamA.latency, streamA.vary)
		readStreamData(b, stream, streamA.bufSize)
		stream.Close()
	}
}

func BenchmarkStreamData2(b *testing.B) {
	for i := 0; i < b.N; i++ {
		stream := newStreamData(streamB.maxSize, streamB.bufSize, streamB.latency, streamB.vary)
		readStreamData(b, stream, streamB.bufSize)
		stream.Close()
	}
}
