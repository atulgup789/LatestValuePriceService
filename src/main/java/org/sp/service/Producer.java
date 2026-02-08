package org.sp.service;

import org.sp.model.PriceRecord;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

final class Producer {
    private final Store store;
    private static final int CHUNK_SIZE = 1000;
    private static final ExecutorService UPLOAD_EXECUTOR = Executors.newFixedThreadPool(8);

    public Producer(Store store) {
        this.store = store;
    }

    // this starts the batch. Gets nextBatchId in atomic increment, so no collisions here
    public String startBatch() {
        String batchId = store.nextBatchId();
        store.putBatch(batchId, new Batch());
        return batchId;
    }

    public void uploadPrices(String batchId, Collection<PriceRecord> records) {
        Batch batch = store.getBatch(batchId);
        if (batch == null) {
            return;
        }
        if (records == null || records.isEmpty()) {
            return;
        }
        if (records.size() <= CHUNK_SIZE) {
            // Batch handles its own concurrency and latest-asOf per id.
            batch.addRecords(records);
            return;
        }
        List<PriceRecord> list = new ArrayList<>(records);
        int total = list.size();
        int chunks = (total + CHUNK_SIZE - 1) / CHUNK_SIZE;
        CompletableFuture<?>[] futures = new CompletableFuture<?>[chunks];
        int index = 0;
        for (int start = 0; start < total; start += CHUNK_SIZE) {
            int end = Math.min(start + CHUNK_SIZE, total);
            List<PriceRecord> slice = list.subList(start, end);
            futures[index++] = CompletableFuture.runAsync(() -> batch.addRecords(slice), UPLOAD_EXECUTOR);
        }
        CompletableFuture.allOf(futures).join();
    }

    // this will first update the batchState to Completed and then commit the batch
    public void completeBatch(String batchId) {
        Batch batch = store.getBatch(batchId);
        if (batch == null) {
            return;
        }
        Map<String, PriceRecord> sealed = batch.sealAndGet();
        if (sealed == null) {
            return;
        }
        store.applyCommittedBatch(sealed);

        //for cleaning the memory
        store.removeBatch(batchId, batch);
    }

    // this will update the batchState to Cancelled
    public void cancelBatch(String batchId) {
        Batch batch = store.getBatch(batchId);
        if (batch == null) {
            return;
        }
        if (batch.cancel()) {
            store.removeBatch(batchId, batch);
        }
    }
}
