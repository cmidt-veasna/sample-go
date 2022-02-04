package main

import (
	"context"
	"fmt"
	"github/cmdit-veasna/sample-go/internal/data"
	"github/cmdit-veasna/sample-go/internal/route"
	"github/cmdit-veasna/sample-go/pkg/config"
	"log"
	"net/http"
	"os/signal"
	"syscall"
)

func main() {
	conf := config.GetConfiguration()

	// listens for the interrupt signal from the OS.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := &http.Server{
		Addr:         fmt.Sprintf("%s:%d", conf.Host, conf.Port),
		ReadTimeout:  conf.ConnectionTimeOut,
		WriteTimeout: conf.ConnectionTimeOut,
		Handler: data.RegisterPool(conf, route.GetRoute(), func(bufferSize int, stop <-chan uint8, jobs <-chan data.EmitterJob) {
			// push data define in EmitterJob to big cloud storage.
		}),
	}

	// Initializing the server in a goroutine and allow main routine to wait for interruption signal
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	// Listen for the interrupt signal.
	<-ctx.Done()

	// Restore default behavior on the interrupt signal thus second signal will not be capture
	stop()

	// notify http.Server that it's a period time to finish any task
	ctx, cancel := context.WithTimeout(context.Background(), conf.GracefulPeriod)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Shutdown: server force to shutdown %s\n", err)
	}
	log.Println("Server exiting")
}
