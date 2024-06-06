package kafka

import (
	"runtime"
	"sync"
)

// By default, there are numCPU*8 workers
var defaultPoolSize = runtime.NumCPU() * 8

type WorkerPool struct {
	queue chan struct{}
	wg    *sync.WaitGroup
}

// NewWorkerPool Create a new pool with (defaultPoolSize) workers
func NewWorkerPool() *WorkerPool {
	return NewWorkerPoolWithSize(defaultPoolSize)
}

// NewWorkerPoolWithSize Create a new pool with a given number of workers
func NewWorkerPoolWithSize(size int) *WorkerPool {
	return &WorkerPool{
		queue: make(chan struct{}, size),
		wg:    &sync.WaitGroup{},
	}
}

// Acquire a worker
// Call to this function will block if there is no worker available in the pool
func (wp *WorkerPool) Acquire() {
	wp.queue <- struct{}{}
	wp.wg.Add(1)
}

// Release a worker
// Call to this function will never block even if the pool is already full
func (wp *WorkerPool) Release() {
	select {
	case <-wp.queue:
		wp.wg.Done()
	}
}

// Wait until all workers have been released
func (wp *WorkerPool) Wait() {
	wp.wg.Wait()
}
