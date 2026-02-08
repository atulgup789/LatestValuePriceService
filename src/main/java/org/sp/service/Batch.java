package org.sp.service;

import org.sp.model.PriceRecord;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;


public final class Batch {
    private final Map<String, PriceRecord> batchData = new HashMap<>();
    private BatchStatus status = BatchStatus.OPEN;

    public synchronized void addRecords(Collection<PriceRecord> records) {
        if (status != BatchStatus.OPEN) {
            return;
        }
        // Keep the latest asOf per id within the batch.
        for (PriceRecord record : records) {
            if (record == null) {
                continue;
            }
            PriceRecord existing = batchData.get(record.id());
            if (existing == null || record.asOf().isAfter(existing.asOf())) {
                batchData.put(record.id(), record);
            }
        }
    }

    public synchronized Map<String, PriceRecord> sealAndGet() {
        if (status != BatchStatus.OPEN) {
            return null;
        }
        status = BatchStatus.COMPLETED;
        // Return a copy to prevent further mutation after sealing.
        return new HashMap<>(batchData);
    }

    public synchronized boolean cancel() {
        if (status != BatchStatus.OPEN) {
            return false;
        }
        status = BatchStatus.CANCELLED;
        batchData.clear();
        return true;
    }

    private enum BatchStatus {
        OPEN,
        COMPLETED,
        CANCELLED
    }
}
