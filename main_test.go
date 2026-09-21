package main

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunSupervisedRestartsAfterPanic(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	var calls atomic.Int32
	restarted := make(chan struct{})

	runSupervised(ctx, &wg, "test", func(ctx context.Context) {
		if calls.Add(1) == 1 {
			panic("boom")
		}
		close(restarted)
		<-ctx.Done()
	})

	select {
	case <-restarted:
	case <-time.After(5 * time.Second):
		t.Fatal("component was not restarted after panic")
	}

	cancel()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("wg.Wait did not return after cancel")
	}

	if got := calls.Load(); got != 2 {
		t.Errorf("run called %d times, want 2", got)
	}
}
