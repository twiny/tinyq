package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"runtime"
	"sync"
	"time"

	"github.com/go-chi/chi"
	"github.com/issmeftah/bprint"
	"github.com/issmeftah/tinyq"
	boltstore "github.com/issmeftah/tinyq/stores/bbolt"
)

var (
	ErrMessageFailed = errors.New("message failed")
)

func main() {
	store, err := boltstore.NewStore("store.db")
	if err != nil {
		log.Println(err)
		return
	}

	// queue
	queue := tinyq.NewQueue(store, false)
	defer queue.Close()

	wg := &sync.WaitGroup{}

	numbers := make(chan int, 2048)
	go func() {
		for i := 0; i < 1000; i++ {
			numbers <- i
		}
		close(numbers)
	}()

	// Enqueue
	wg.Add(20)
	start := time.Now()
	for j := 0; j < 20; j++ {
		go func() {
			for i := range numbers {
				queue.Enqueue(i)
			}
			wg.Done()
		}()
	}

	// Dequeue
	wg.Add(1)
	for i := 0; i < 1; i++ {
		go func() {
			for msg := range queue.Dequeue() {
				var j int
				if err := msg.Value(&j); err != nil {
					fmt.Println(err)
					continue
				}

				if (j % 2) == 0 {
					fmt.Println("failed", j)
					queue.Notify(msg.UUID, ErrMessageFailed)
					continue
				}
				fmt.Println("success", j)
				queue.Notify(msg.UUID, nil)
			}
			wg.Done()
		}()
	}

	go func() {
		wg.Wait()
		fmt.Println(time.Since(start))
	}()

	// HTTP Server
	router := chi.NewRouter()

	router.Get("/pause", func(w http.ResponseWriter, r *http.Request) {
		if err := queue.Exec(tinyq.Pause); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		w.Write([]byte("queue paused."))
	})

	router.Get("/start", func(w http.ResponseWriter, r *http.Request) {
		if err := queue.Exec(tinyq.Start); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		w.Write([]byte("queue started."))
	})

	router.Get("/stats", func(w http.ResponseWriter, r *http.Request) {
		stats, err := queue.Statistic()
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		b, _ := json.Marshal(stats)
		w.Write(b)
	})

	router.Get("/debug", func(w http.ResponseWriter, r *http.Request) {
		stats := new(runtime.MemStats)
		runtime.ReadMemStats(stats)

		// app stats
		astats := map[string]interface{}{
			"Goroutine":   runtime.NumGoroutine(),
			"CPUs":        runtime.NumCPU(),
			"System":      bprint.String(stats.Sys),
			"StackSystem": bprint.String(stats.StackSys),
			"GCSize":      bprint.String(stats.GCSys),
			"CompletedGC": stats.NumGC,
		}

		b, _ := json.Marshal(astats)
		w.Write(b)
	})

	srv := &http.Server{
		Addr:    ":http",
		Handler: router,
	}

	log.Println(srv.ListenAndServe())
	//
	log.Println("done.")
}
