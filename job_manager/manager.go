package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"job_manager/wrapper_classes"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/joho/godotenv"
	"github.com/weaviate/weaviate-go-client/v4/weaviate"
	"github.com/weaviate/weaviate-go-client/v4/weaviate/auth"
)

type Context struct {
	wg         sync.WaitGroup
	statusLock sync.Mutex
	jobMap     map[uint64]Job
	jobQueue   chan Job
	jobCounter atomic.Uint64
}

type Job struct {
	ID          uint64
	WeaviateIDs []string
	startTime   time.Time
	healthTime  time.Time
	// status      Status
}

type JobArgs struct {
	ID uint64
}

var ctx Context

func main() {
	groupSize := 2
	miniBatchSize := 10
	healthInterval := 2
	healthTimeout := 10

	ctx = Context{
		jobMap:   make(map[uint64]Job),
		jobQueue: make(chan Job, groupSize),
	}
	go QueueJobs(groupSize, miniBatchSize, "")
	go HealthRoutine(time.Duration(healthInterval), time.Duration(healthTimeout))

	mux := http.NewServeMux()
	mux.HandleFunc("POST /request_job", RequestJob)
	mux.HandleFunc("POST /finish_job", FinishJob)
	mux.HandleFunc("POST /health_check", HealthCheck)
	log.Println("Listening on http://localhost:8080/")
	log.Fatal(http.ListenAndServe(":8080", mux))
}

func FinishJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method is not supported.", http.StatusMethodNotAllowed)
		return
	}

	var args JobArgs
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&args); err != nil {
		http.Error(w, "Error parsing JSON from request body\n"+err.Error(), http.StatusBadRequest)
		return
	}

	// remove job from jobMap and ignore if it isn't there (possible if job timed out)
	ctx.statusLock.Lock()
	defer ctx.statusLock.Unlock()
	job, ok := ctx.jobMap[args.ID]
	if ok {
		log.Printf("Job %v finished [%.2f seconds].", args.ID, time.Since(job.startTime).Seconds())
		delete(ctx.jobMap, args.ID)
		ctx.wg.Done()
	}
}

func HealthCheck(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method is not supported.", http.StatusMethodNotAllowed)
		return
	}

	var args JobArgs
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&args); err != nil {
		http.Error(w, "Error parsing JSON from request body\n"+err.Error(), http.StatusBadRequest)
		return
	}

	// remove job from jobMap and ignore if it isn't there (possible if job timed out)
	ctx.statusLock.Lock()
	defer ctx.statusLock.Unlock()
	job, ok := ctx.jobMap[args.ID]
	if ok {
		log.Printf("Job %v is healthy.", args.ID)
		job.healthTime = time.Now()
		ctx.jobMap[args.ID] = job
		ctx.wg.Done()
	}
}

func RequestJob(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method is not supported.", http.StatusMethodNotAllowed)
		return
	}

	// pop new job from queue
	job := <-ctx.jobQueue
	currentTime := time.Now()
	job.healthTime = currentTime
	job.startTime = currentTime

	log.Printf("Job %v requested.\n", job.ID)

	ctx.statusLock.Lock()
	defer ctx.statusLock.Unlock()
	ctx.jobMap[job.ID] = job

	// output job as json (only ID and WeaviateIDs fields will be encoded)
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(job); err != nil {
		http.Error(w, "Error writing JSON response\n"+err.Error(), http.StatusInternalServerError)
		return
	}
}

func HealthRoutine(healthInterval time.Duration, healthTimeout time.Duration) {
	// ensure all current jobs are healthy (not timed out)
	for {
		time.Sleep(healthInterval * time.Second)
		ctx.statusLock.Lock()
		currentTime := time.Now()
		for _, job := range ctx.jobMap {
			if job.healthTime.Before(currentTime.Add(-healthTimeout * time.Second)) {
				// requeue job if it hasn't recieved a health check in healthTimeout seconds
				// TODO: should this new job have a different id?
				log.Printf("Job %v has timed out.", job.ID)
				ctx.jobQueue <- job
				delete(ctx.jobMap, job.ID)
			}
		}
		ctx.statusLock.Unlock()
	}
}

func QueueJobs(groupSize int, batchSize int, cursor string) {
	// load environment variables
	env, err := godotenv.Read(".env")
	if err != nil {
		panic(err)
	}
	ClearFile("ids.txt")
	ClearFile("cursor.txt")

	// connect to weaviate
	host := fmt.Sprintf("%v:%v", env["WEAVIATE_HOST"], env["WEAVIATE_PORT"])
	cfg := weaviate.Config{
		Host:       host,
		Scheme:     "http",
		AuthConfig: auth.ApiKey{Value: env["WEAVIATE_API_KEY"]},
		Headers:    nil,
	}
	weaviate_client, err := weaviate.NewClient(cfg)
	if err != nil {
		panic(err)
	}

	// generate jobs in groups and wait for group to finish before moving on to next group
	groupCount := 1
	for {
		groupStart := time.Now()
		var idList []string
		for range groupSize {
			miniBatch, end := wrapper_classes.GenerateFilteredBatch(weaviate_client, batchSize, &cursor)
			id := ctx.jobCounter.Add(1)
			ctx.jobQueue <- Job{
				ID:          id,
				WeaviateIDs: miniBatch,
			}
			idList = append(idList, miniBatch...)
			ctx.wg.Add(1)

			if end {
				log.Println("Extracted all ids from Weaviate.")
				return
			}
		}
		ctx.wg.Wait()
		log.Printf("Finished processing group %v [avg %.2f seconds per paragraph]", groupCount, time.Since(groupStart).Seconds()/float64(groupSize*batchSize))
		SaveToFile("ids.txt", idList)
		SaveToFile("cursor.txt", []string{cursor})
		groupCount++
	}
}

func ClearFile(filename string) {
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Failed to clear file: %s", err)
	}
	file.Close()
}

func SaveToFile(filename string, list []string) {
	// append instead of truncate
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to create file: %s", err)
	}
	defer file.Close()

	writer := bufio.NewWriter(file)
	for _, id := range list {
		_, err := writer.WriteString(id + "\n")
		if err != nil {
			log.Fatalf("Failed to write to file: %s", err)
		}
	}
	writer.Flush()
}
