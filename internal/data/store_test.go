package data

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github/cmdit-veasna/sample-go/pkg/config"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type poolTestCase struct {
	conf    config.Configuration
	readers [][]byte
	cancel  func([]*task)
	pause   time.Duration
}

type task struct {
	id     string
	data   []byte
	ctx    context.Context
	cancel context.CancelFunc
	done   <-chan error
}

// In store, only the below configuration is being use thus we don't need to define other value
var defConf = config.Configuration{
	RecieveWorker:  10,
	EmitterWorker:  5,
	TemporayFolder: filepath.Join(os.TempDir(), "data"),
	BufferSize:     config.ByteSize{Size: 1024 * 400}, // 400MB
}

// defEmitterWorker is an emitter worker that read the data from cache file and
// return the total data if nothing wrong. The worker will sleep for each time
// after read a chuck of data from cache file.
func defEmitterWorker(bufferSize int, pause time.Duration, discard bool, stop <-chan uint8, job EmitterJob) ([]byte, error) {
	file, err := os.Open(job.file)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var data *bytes.Buffer
	var store io.Writer
	if discard {
		store = ioutil.Discard
		data = bytes.NewBuffer(make([]byte, 0))
	} else {
		data = bytes.NewBuffer(make([]byte, 0, bufferSize*2))
		store = data
	}

	buf := make([]byte, bufferSize)

	for {
		select {
		case <-stop:
			// pool has forcefully shutdown
			return nil, errors.New("worker shutdown")
		default:
			if n, err := file.Read(buf); err == io.EOF {
				store.Write(buf[:n])
				return data.Bytes(), nil
			} else if err != nil {
				return nil, err
			} else {
				store.Write(buf[:n])
			}
			time.Sleep(pause)
		}
	}
}

// byte size constant
const (
	fiveMB    = 5 * 1024
	tenMB     = 10 * 1024
	fiftyMB   = 50 * 1024
	hundrenMB = 100 * 1024
	hundrenGB = 100 * 1024 * 1024
)

// generateByteData generate random byte data based on the given `preferSize` and `vary`.
func generateByteData(preferSize, vary int) []byte {
	total := preferSize + rand.Intn(vary)
	buf := make([]byte, total)
	n, err := rand.Read(buf)
	// panic: it look like we don't have enough memery
	if err != nil {
		panic(err)
	} else if n < preferSize {
		panic(fmt.Sprintf("generate byte %d is less than prefer size %d", n, preferSize))
	}
	return buf
}

var poolTests = []*poolTestCase{
	{ // case 1
		conf: defConf,
		readers: [][]byte{
			generateByteData(fiftyMB, tenMB),
			generateByteData(fiftyMB, tenMB),
			generateByteData(hundrenMB, tenMB),
		},
		pause: time.Millisecond * 200,
	},
	{ // case 2
		conf: defConf,
		readers: [][]byte{
			generateByteData(tenMB, fiveMB),
			generateByteData(fiftyMB, tenMB),
			generateByteData(fiftyMB, tenMB),
			generateByteData(hundrenMB, tenMB),
		},
		pause: time.Millisecond * 200,
	},
	{ // case 3: cancel or timeout
		conf: config.Configuration{
			RecieveWorker:  2,
			EmitterWorker:  1,
			TemporayFolder: filepath.Join(os.TempDir(), "data"),
			BufferSize:     config.ByteSize{Size: 1024}, // 1MB
		},
		readers: [][]byte{
			generateByteData(fiftyMB, tenMB),
			generateByteData(fiftyMB, tenMB),
			generateByteData(hundrenMB, tenMB),
		},
		cancel: func(tasks []*task) {
			for _, task := range tasks {
				task.cancel()
			}
		},
		pause: time.Second * 5,
	},
}

func TestPoolWorker(t *testing.T) {
	log.SetOutput(ioutil.Discard)
	var emitterDone = make(chan *task, 3)
	for i, tc := range poolTests {
		t.Logf("TestPoolWorker case #%d", i+1)
		pool := NewPool(tc.conf, func(bufferSize int, stop <-chan uint8, jobs <-chan EmitterJob) {
		finish:
			for {
				select {
				case <-stop:
					break finish
				case job, ok := <-jobs:
					if !ok {
						// channel closed
						return
					}
					data, err := defEmitterWorker(bufferSize, tc.pause, false, stop, job)
					assert.NoError(t, err)
					emitterDone <- &task{id: job.id, data: data}
					assert.NoError(t, os.Remove(job.file))
				}
			}
		})
		var cp = pool
		cp.StartPool()

		tasks := make([]*task, len(tc.readers))
		// broadcast the jobs to the pool
		for ir, r := range tc.readers {
			ctx, cancel := context.WithCancel(context.Background())
			id := fmt.Sprintf("job::%d::%d", i+1, ir+1)
			tasks[ir] = &task{
				id:     id,
				ctx:    ctx,
				cancel: cancel,
				data:   r,
				done:   cp.RecieverJob(ctx, id, bytes.NewReader(r)),
			}
		}

		if tc.cancel != nil {
			go tc.cancel(tasks)
		}

		// waiting for the result from the pool
		// we don't need to urgently handle any of the finished recieving job
		// we only care if worker does it jobs properly

		// verify if reciever work finish the job properly
		for _, task := range tasks {
			err := <-task.done
			task.cancel()
			if tc.cancel != nil {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		}

		// verify that emitter read the data properly in case the cancel is not happen
		if tc.cancel == nil {
			production := make(map[string][]byte)
			// wait for all emitter to finish
			for i := 0; i < len(tc.readers); i++ {
				// we don't need to know the data from channel emitterDone
				// the production result must not nil if emitter finish it work
				task, ok := <-emitterDone
				require.True(t, ok)
				require.NotNil(t, task)
				production[task.id] = task.data
			}

			// verify whether the emitter read the exact data that was given to the reciever
			for _, task := range tasks {
				require.NotNil(t, production[task.id])
				assert.Equal(t, task.data, production[task.id])
			}
		}
		// no more jobs to emit
		cp.Shutdown()
	}
	close(emitterDone)
}

func runSamplePoolBench(b *testing.B, ignoreEmitter bool, data []*StreamData, count int, conf config.Configuration) {
	emitterDone := make(chan uint8, 10)
	defer close(emitterDone)
	pool := NewPool(conf, func(bufferSize int, stop <-chan uint8, jobs <-chan EmitterJob) {
		defer func() {
			emitterDone <- 0
		}()
		for {
			select {
			case <-stop:
				return
			case job, ok := <-jobs:
				if !ok {
					// channel closed
					return
				}
				_, err := defEmitterWorker(bufferSize, 0, true, stop, job)
				assert.NoError(b, err)
				if !ignoreEmitter {
					emitterDone <- 1
				}
			}
		}
	})
	var cp = pool
	cp.StartPool()

	// wait for all reciever job done
	var dones = make(chan (<-chan error), 10)
	var wg sync.WaitGroup
	wg.Add(1)
	if !ignoreEmitter {
		wg.Add(1)
		go func() {
			for i := 0; i < count; i++ {
				<-emitterDone
			}
			wg.Done()
		}()
	}
	go func() {
		for i := 0; i < count; i++ {
			<-<-dones
		}
		wg.Done()
	}()

	// wait for all emitter job done
	// regardless of whether benchmark care about overall task duration & performance
	// we need to feed emitter channel.

	for i := 0; i < count; i++ {
		dones <- cp.RecieverJob(
			context.Background(),
			fmt.Sprintf("job::%d", i+1),
			data[i],
		)
	}
	// all task complete
	wg.Wait()
}

var (
	conf1 = config.Configuration{
		RecieveWorker:  40,
		EmitterWorker:  10,
		TemporayFolder: filepath.Join(os.TempDir(), "data"),
		BufferSize:     config.ByteSize{Size: 1024 * 100}, // 100MB
	}
	conf2 = config.Configuration{
		RecieveWorker:  40,
		EmitterWorker:  10,
		TemporayFolder: filepath.Join(os.TempDir(), "data"),
		BufferSize:     config.ByteSize{Size: 1024 * 500}, // 500MB
	}
)

func closeStreamData(streamSet []*StreamData) {
	for _, ds := range streamSet {
		ds.Close()
	}
}

func generateRandomData(count, size, vary, bufSize int) []*StreamData {
	data := make([]*StreamData, count)
	for i := 0; i < count; i++ {
		data[i] = newStreamData(rand.Intn(vary)+size, bufSize, 0, 0)
	}
	return data
}

func BenchmarkPool1aWithEmitter(b *testing.B) {
	log.SetOutput(ioutil.Discard)
	// 50 concurrent input data of ~100MB in average.
	// benchmark measure until emitter jobs done
	for i := 0; i < b.N; i++ {
		streamSet := generateRandomData(50, hundrenMB, fiftyMB, 4*hundrenMB)
		runSamplePoolBench(b, false, streamSet, len(streamSet), conf1)
		closeStreamData(streamSet)
	}
}
func BenchmarkPool1bNoEmitter(b *testing.B) {
	log.SetOutput(ioutil.Discard)
	// 50 concurrent input data of ~100MB in average.
	// benchmark measure only reciever jobs done
	for i := 0; i < b.N; i++ {
		streamSet := generateRandomData(50, hundrenMB, fiftyMB, 4*hundrenMB)
		runSamplePoolBench(b, true, streamSet, len(streamSet), conf1)
		closeStreamData(streamSet)
	}
}

func BenchmarkPool2aWithEmitter(b *testing.B) {
	log.SetOutput(ioutil.Discard)
	// 10 concurrent input data of ~1GB in average.
	// benchmark measure until emitter jobs done
	for i := 0; i < b.N; i++ {
		streamSet := generateRandomData(10, hundrenMB*120, hundrenMB, 4*hundrenMB) // ~1GB
		runSamplePoolBench(b, false, streamSet, len(streamSet), conf2)
		closeStreamData(streamSet)
	}
}
func BenchmarkPool2bNoEmitter(b *testing.B) {
	log.SetOutput(ioutil.Discard)
	// 10 concurrent input data of ~1GB in average.
	// benchmark measure on reciever jobs done
	for i := 0; i < b.N; i++ {
		streamSet := generateRandomData(10, hundrenMB*120, hundrenMB, 4*hundrenMB) // ~1GB
		runSamplePoolBench(b, true, streamSet, len(streamSet), conf2)
		closeStreamData(streamSet)
	}
}

func BenchmarkPool3aWithEmitter(b *testing.B) {
	log.SetOutput(ioutil.Discard)
	// 10 concurrent input data of 100GB in average.
	// benchmark measure until emitter jobs done
	for i := 0; i < b.N; i++ {
		streamSet := generateRandomData(10, hundrenGB, hundrenMB, 4*hundrenMB)
		runSamplePoolBench(b, false, streamSet, len(streamSet), conf2)
		closeStreamData(streamSet)
	}
}
func BenchmarkPool3bNoEmitter(b *testing.B) {
	log.SetOutput(ioutil.Discard)
	// 10 concurrent input data of 100GB in average.
	// benchmark measure only reciever jobs done
	for i := 0; i < b.N; i++ {
		streamSet := generateRandomData(10, hundrenGB, hundrenMB, 4*hundrenMB)
		runSamplePoolBench(b, true, streamSet, len(streamSet), conf2)
		closeStreamData(streamSet)
	}
}
