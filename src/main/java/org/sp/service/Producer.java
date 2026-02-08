package org.sp.service;

import org.sp.model.PriceRecord;

import java.util.Collection;
import java.util.Map;

final class Producer {
    private final Store store;

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
        // Batch handles its own concurrency and latest-asOf per id.
        batch.addRecords(records);
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
