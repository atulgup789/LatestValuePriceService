# Latest Value Price Service

In-memory Java service for tracking last prices of financial instruments with
batch uploads and atomic visibility on completion.

## Features
- Batch lifecycle: start, upload in chunks, complete or cancel
- Atomic publish of completed batches only
- Last price chosen by `asOf` timestamp
- Resilient to out-of-order producer calls


## Build and Test
Run tests with Maven:
```
mvn test
```

## Notes
- Data is kept in memory only.
- Consumers never see partial batch data; only completed batches are visible.
- Uploads over 1000 records are split into 1000-sized chunks and processed in parallel on a bounded thread pool.