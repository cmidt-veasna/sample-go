package data

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"

	"github/cmdit-veasna/sample-go/pkg/config"

	"github.com/gin-gonic/gin"
	uuid "github.com/satori/go.uuid"
)

// poolKey use as key to with value pool instance in gin.Context
const poolKey = "worker-pool"

// RegisterPool add middleware to `mux` to apply bind pool instance.
func RegisterPool(conf config.Configuration, mux *gin.Engine, emitterWorker EmitterWorker) *gin.Engine {
	pool := NewPool(conf, emitterWorker)
	mux.Use(pool.serve)
	return mux
}

// GetPool return an instance of pool from the context.
// It is return error if the instance is not available or it was not
// pool instance.
func GetPool(ctx *gin.Context) (*pool, error) {
	val, ok := ctx.Get(poolKey)
	if !ok {
		return nil, errors.New("pool not found in the context")
	} else if p, ok := val.(*pool); !ok {
		return nil, fmt.Errorf("invalid pool instance %v", val)
	} else {
		return p, nil
	}
}

type recieverJob struct {
	id     string
	reader io.Reader
	ctx    context.Context
	done   chan<- error // a channel result of reciever job
}

// EmitterJob represent metadata of each request job for emitter
type EmitterJob struct {
	id   string
	file string
}

// EmitterWorker a handler function that handle transferring input data
// from temporary folder to cloud storage.
type EmitterWorker func(bufferSize int, stop <-chan uint8, jobs <-chan EmitterJob)

// Pool represent instance of worker pool channel
type pool struct {
	conf          config.Configuration
	reciever      chan recieverJob
	emitter       chan EmitterJob
	emitterWorker EmitterWorker
	stop          chan uint8
	lock          sync.RWMutex
}

// NewPool create new pool instance
func NewPool(conf config.Configuration, emitterWorker EmitterWorker) *pool {
	return &pool{
		conf:          conf,
		reciever:      make(chan recieverJob, conf.RecieveWorker*2),
		emitter:       make(chan EmitterJob, conf.EmitterWorker*2),
		emitterWorker: emitterWorker,
		stop:          make(chan uint8),
	}
}

// serve implement `gin.Engine` middleware to bind instance into context.
func (pool *pool) serve(ctx *gin.Context) { ctx.Set(poolKey, pool) }

// StartPool start reciever and emitter worker pool
func (pool *pool) StartPool() {
	// create receiver and emitter worker pool
	if err := os.MkdirAll(pool.conf.TemporayFolder, 0700); err != nil {
		log.Println("create temporary folder failed", err.Error())
		os.Exit(0)
	}
	go pool.startRecieveWorker()
	pool.startEmitterWorker()
}

// Shutdown notify worker to stop waiting for jobs and goroutine is execution is done.
func (pool *pool) Shutdown() {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	pool.stop <- 0
	close(pool.stop)
	close(pool.reciever)
}

// RecieverJob push the job into Reciever job channel
func (pool *pool) RecieverJob(ctx context.Context, id string, reader io.Reader) <-chan error {
	pool.lock.Lock()
	defer pool.lock.Unlock()
	result := make(chan error)
	pool.reciever <- recieverJob{
		id:     id,
		reader: reader,
		ctx:    ctx,
		done:   result,
	}
	return result
}

// startRecieveWorker create multiple infinite go routine waiting for
// http client request arrive the server
func (pool *pool) startRecieveWorker() {
	for i := 0; i < pool.conf.RecieveWorker; i++ {
		go pool.recieverWorker(i + 1)
	}
	<-pool.stop
	close(pool.emitter)
}

// startEmitterWorker create multiple infinte go routine waiting for
// any available job to update data to big storage
func (pool *pool) startEmitterWorker() {
	for i := 0; i < pool.conf.EmitterWorker; i++ {
		go pool.emitterWorker(int(pool.conf.BufferSize.Size), pool.stop, pool.emitter)
	}
}

// recieverWorker read data from http client and save it into temporary file
func (pool *pool) recieverWorker(id int) {
	var buf = make([]byte, int(pool.conf.BufferSize.Size))
	var numRead, numWrite int
	log.Printf("reciever worker id %d start accepting job", id)
shutdown:
	for {
		select {
		case <-pool.stop:
			break shutdown
		case job, ok := <-pool.reciever:
			if !ok {
				// channel close
				break shutdown
			}
			var filename = filepath.Join(pool.conf.TemporayFolder, base64.RawURLEncoding.EncodeToString(uuid.NewV4().Bytes()[:]))
			file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY, 0700)
			if err != nil {
				finishJob(job.done, err)
			}
		done:
			for {
				select {
				case <-pool.stop:
					file.Close()
					os.Remove(filename)
					log.Printf("reciever worker id %d job canceled on cache file %s due to shutdown request", id, filename)
					finishJob(job.done, fmt.Errorf("job: id %s is being aborted", job.id))
					break shutdown
				case <-job.ctx.Done():
					// job cancel delete any saving data
					file.Close()
					os.Remove(filename)
					log.Printf("reciever worker id %d job canceled on cache file %s", id, filename)
					finishJob(job.done, fmt.Errorf("job: id %s is being canceled", job.id))
					break done
				default:
					numRead, err = job.reader.Read(buf)
					if err != nil && err != io.EOF {
						finishJob(job.done, err)
						break done
					}
					if numWrite, err = file.Write(buf[:numRead]); err != nil {
						finishJob(job.done, err)
					} else if numWrite != numRead {
						finishJob(job.done, errors.New("unable to write post data"))
					} else if numRead < int(pool.conf.BufferSize.Size) {
						file.Close()
						finishJob(job.done, nil)
						// all data have well recieve, let push job to emitter to transfer
						// data to cloud storage.
						pool.emitter <- EmitterJob{
							id:   job.id,
							file: filename,
						}
						log.Printf("reciever worker id %d finish caching to file %s", id, filename)
						break done
					}
				}
			}
		}
	}
	log.Printf("reciever worker id %d exiting", id)
}

func finishJob(done chan<- error, err error) {
	done <- err
	close(done)
}
