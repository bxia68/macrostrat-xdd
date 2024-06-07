package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"job_manager/data_loaders"
	"log"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

type JobType string

const (
	Wait        JobType = "wait"
	BatchJob    JobType = "batch"
	OnDemandJob JobType = "on_demand"
)

type Context struct {
	wg            sync.WaitGroup
	statusLock    sync.Mutex
	jobMap        map[uint64]Job
	jobQueue      chan Job
	jobCounter    atomic.Uint64
	healthTimeout int
	metadata      JobMetadata
}

type JobMetadata struct {
	PipelineId int
	RunID      int
}

type Job struct {
	JobType       JobType
	ID            uint64
	JobData       []string
	Metadata      JobMetadata
	HealthTimeout int
	returnChan    chan map[string]interface{}
	startTime     time.Time
	healthTime    time.Time
}

type JobArgs struct {
	ID     uint64
	Result map[string]interface{}
}

type JobRequest struct {
	JobData []string
}

type DataLoader interface {
	InitValues()
	GetBatch(int) ([]string, bool)
}

var ctx Context

func main() {
	groupSize, _ := strconv.Atoi(os.Getenv("GROUP_SIZE"))
	miniBatchSize, _ := strconv.Atoi(os.Getenv("MINI_BATCH_SIZE"))
	healthInterval, _ := strconv.Atoi(os.Getenv("HEALTH_INTERVAL"))
	healthTimeout, _ := strconv.Atoi(os.Getenv("HEALTH_TIMEOUT"))
	pipelineId, _ := strconv.Atoi(os.Getenv("PIPELINE_ID"))
	runId, _ := strconv.Atoi(os.Getenv("RUN_ID"))

	ctx = Context{
		jobMap:        make(map[uint64]Job),
		jobQueue:      make(chan Job, groupSize),
		healthTimeout: healthTimeout,
		metadata:      JobMetadata{PipelineId: pipelineId, RunID: runId},
	}

	loader_flag := flag.String("loader", "", "Specify the data loader for the manager. Options are \"weaviate\", \"map_descrip\". Default is none (on demand queuing).")
	flag.Parse()
	var loader DataLoader
	switch *loader_flag {
	case "weaviate":
		loader = &data_loaders.WeaviateLoader{}
	case "map_descrip":
		loader = &data_loaders.DescriptionsLoader{}
	}
	if loader != nil {
		loader.InitValues()
		go QueueJobs(groupSize, miniBatchSize, loader)
	}

	go HealthRoutine(time.Duration(healthInterval), time.Duration(healthTimeout))

	mux := http.NewServeMux()
	mux.HandleFunc("POST /request_job", RequestJob)
	mux.HandleFunc("POST /finish_job", FinishJob)
	mux.HandleFunc("POST /health_check", HealthCheck)
	mux.HandleFunc("POST /submit_job", SubmitJob)
	log.Println("Listening on http://localhost:8000/")
	log.Fatal(http.ListenAndServe(":8000", mux))
}

func SubmitJob(w http.ResponseWriter, r *http.Request) {
	var job JobRequest
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&job); err != nil {
		http.Error(w, "Error parsing JSON from request body\n"+err.Error(), http.StatusBadRequest)
		return
	}

	// add new job to channel
	id := ctx.jobCounter.Add(1)
	returnChan := make(chan map[string]interface{})
	ctx.jobQueue <- Job{
		ID:         id,
		JobType:    OnDemandJob,
		JobData:    job.JobData,
		returnChan: returnChan,
	}
	log.Printf("Accepted request to queued job of %v paragraphs", len(job.JobData))

	result := <-returnChan

	// output result as json
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		http.Error(w, "Error writing JSON response\n"+err.Error(), http.StatusInternalServerError)
	}
}

func FinishJob(w http.ResponseWriter, r *http.Request) {
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
		if job.JobType == OnDemandJob {
			job.returnChan <- args.Result
		} else {
			ctx.wg.Done()
		}

		log.Printf("Job %v finished [%.2f seconds].", args.ID, time.Since(job.startTime).Seconds())
		delete(ctx.jobMap, args.ID)
	}
}

func HealthCheck(w http.ResponseWriter, r *http.Request) {
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
	}
}

func RequestJob(w http.ResponseWriter, r *http.Request) {
	// pop new job from channel, tell worker to wait if no jobs are available
	var job Job
	select {
	case job = <-ctx.jobQueue:
	case <-time.After(1 * time.Second):
		job.JobType = Wait
		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(job); err != nil {
			http.Error(w, "Error writing JSON response\n"+err.Error(), http.StatusInternalServerError)
		}
		return
	}

	// update state of job
	currentTime := time.Now()
	job.healthTime = currentTime
	job.startTime = currentTime
	job.HealthTimeout = ctx.healthTimeout / 10
	log.Printf("Job %v requested.\n", job.ID)

	ctx.statusLock.Lock()
	defer ctx.statusLock.Unlock()
	ctx.jobMap[job.ID] = job

	// output job as json
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(job); err != nil {
		http.Error(w, "Error writing JSON response\n"+err.Error(), http.StatusInternalServerError)
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

func QueueJobs(groupSize int, batchSize int, dataLoader DataLoader) {
	// generate jobs in groups and wait for group to finish before moving on to next group
	groupCount := 1
	ClearFile("processed_data.txt") // may want to adjust how to keep track of processed data (ie saving only offsets)
	for {
		groupStart := time.Now()
		var dataList []string
		for range groupSize {
			miniBatch, end := dataLoader.GetBatch(batchSize)
			id := ctx.jobCounter.Add(1)
			ctx.jobQueue <- Job{
				JobType: BatchJob,
				ID:      id,
				JobData: miniBatch,
			}

			dataList = append(dataList, miniBatch...)
			ctx.wg.Add(1)

			if end {
				log.Println("Queued all jobs.")

				ctx.wg.Wait()
				log.Println("Finished all jobs.")
				return
			}
		}

		ctx.wg.Wait()
		log.Printf("Finished processing group %v [avg %.2f seconds per paragraph]", groupCount, time.Since(groupStart).Seconds()/float64(groupSize*batchSize))
		SaveToFile("processed_data.txt", dataList)
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
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644) // append instead of truncate
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
