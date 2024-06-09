# Job Manager

Job Manager is a Go application designed to manage and schedule jobs concurrently. 
It supports both job queueing from various data sources like Weaviate and Macrostrat, or directly via POST requests. 
This system is structured to work with worker nodes that execute and store results of jobs.

## Extra Details
- **Health Checks:** Regular health checks are performed to ensure jobs have not timed out. 
- **Data Loaders:** Supports different data loaders which can be specified at runtime. 

## Getting Started

### Running the Application

1. Set the necessary environment variables:
   - `GROUP_SIZE`: Defines the number of jobs processed in a batch. The manager will not queue a new group until the current one is completed.
   - `MINI_BATCH_SIZE`: Specifies the number of tasks in each job (ie. number of paragraphs in a job)
   - `HEALTH_INTERVAL`: Time interval in seconds for how often the manager checks for timed out jobs.
   - `HEALTH_TIMEOUT`: Duration in seconds after which a job is considered timed out if no health check is received. Jobs will be requeued if they have timed out.
   - `PIPELINE_ID`: The ID of the pipeline. 
   - `RUN_ID`: The ID of the current run.

2. Run the application:
  - Build the manager:
```docker build -t job-manager .```
  - Run the container in on-demand mode:
```docker run -it --env-file .env -p 8000:8000 job-manager```
  - Optionally, specify a data loader when running the container (currently supports "weaviate" and "map_descrip"):
```docker run -it --env-file .env -p 8000:8000 job-manager --loader=weaviate```

## API Endpoints

- `POST /submit_job`: Submit a new job.
  - Example body:
    ```json
    {"JobData": ["00000b77-48bc-4ed2-accb-6dc5c393202e", "000011c7-3353-4588-8a5c-e3b783c825c6"]}
    ```
