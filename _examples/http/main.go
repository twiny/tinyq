package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"sync"
	"time"
	"tinyq"

	"tinyq/pkg/metrics"
	boltstore "tinyq/stores/bbolt"

	"github.com/go-chi/chi"
	"github.com/go-chi/chi/middleware"
)

func main() {
	// store, err := badgerstore.NewBadgerStore("_test")
	// if err != nil {
	// 	log.Println(err)
	// 	return
	// }

	store, err := boltstore.NewStore("_testx")
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
		for i := 0; i < 1; i++ {
			numbers <- i
		}
		close(numbers)
	}()

	// Enqueue
	wg.Add(50)
	start := time.Now()
	for j := 0; j < 50; j++ {
		go func() {
			for i := range numbers {
				if err := queue.Enqueue(i); err != nil {
					fmt.Println(fmt.Errorf("main 1 %s", err.Error()))
					return
				}
			}
			wg.Done()
		}()
	}

	go func() {
		for err := range queue.Errs() {
			fmt.Println(fmt.Errorf("main queue error %s", err.Error()))
		}
	}()

	go func() {
		wg.Wait()
		fmt.Println(time.Since(start))
	}()

	// Dequeue
	// wg.Add(1)
	for i := 0; i < 10; i++ {
		go func() {
			for msg := range queue.Dequeue() {
				var j int
				if err := msg.Value(&j); err != nil {
					fmt.Println(fmt.Errorf("queue dequeue 1 %s", err.Error()))
					continue
				}

				if (j % 2) == 0 {
					fmt.Println("failed", j)
					if err := queue.Notify(msg.UUID, fmt.Errorf("message %s failed", msg.UUID)); err != nil {
						fmt.Println(fmt.Errorf("queue dequeue 2 %s", err.Error()))
					}
					continue
				}
				fmt.Println("success", j)
				if err := queue.Notify(msg.UUID, nil); err != nil {
					fmt.Println(fmt.Errorf("queue dequeue 3 %s", err.Error()))
				}

				//
				// time.Sleep(300 * time.Millisecond)
			}
			// wg.Done()
		}()
	}

	// HTTP Server
	router := chi.NewRouter()
	router.Use(middleware.SetHeader("Content-Type", "application/json"))

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

		s := map[string]interface{}{
			"queue":   stats,
			"metrics": metrics.GetMetrics(),
		}

		b, _ := json.Marshal(s)
		w.Write(b)
	})

	router.Get("/shutdown", func(w http.ResponseWriter, r *http.Request) {
		queue.Close()
		os.Exit(0)
	})

	srv := &http.Server{
		Addr:    ":http",
		Handler: router,
	}

	log.Println(srv.ListenAndServe())
	//
	log.Println("done.")
}
