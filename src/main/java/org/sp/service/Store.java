package org.sp.service;

import org.sp.model.PriceRecord;

import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public final class Store {

    private final ConcurrentHashMap<String, Batch> batches = new ConcurrentHashMap<>();

    //volatile keyword will ensure changes are instantly visible to the client
    private volatile Map<String, PriceRecord> committedData = Collections.emptyMap();

    private final Object commitLock = new Object();

    private final AtomicLong batchSequence = new AtomicLong(0);

    public String nextBatchId() {
        return "Batch-" + batchSequence.incrementAndGet();
    }

    public Batch getBatch(String batchId) {
        return batches.get(batchId);
    }

    public void putBatch(String batchId, Batch state) {
        batches.put(batchId, state);
    }

    public void removeBatch(String batchId, Batch expected) {
        batches.remove(batchId, expected);
    }

    public Map<String, PriceRecord> snapshot() {
        return committedData;
    }

    public void applyCommittedBatch(Map<String, PriceRecord> sealed) {
        synchronized (commitLock) {
            Map<String, PriceRecord> updated = new HashMap<>(committedData);
            for (PriceRecord record : sealed.values()) {
                PriceRecord existing = updated.get(record.id());
                if (existing == null || isAfter(record.asOf(), existing.asOf())) {
                    updated.put(record.id(), record);
                }
            }
            committedData = Collections.unmodifiableMap(updated);
        }
    }

    private boolean isAfter(Instant candidate, Instant existing) {
        return candidate.isAfter(existing);
    }
}
