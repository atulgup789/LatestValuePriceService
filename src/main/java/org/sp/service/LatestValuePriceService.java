package org.sp.service;

import org.sp.model.PriceRecord;

import java.util.Collection;
import java.util.Map;

public class LatestValuePriceService {
    private final Producer producer;
    private final Consumer consumer;

    public LatestValuePriceService() {
        Store store = new Store();
        producer = new Producer(store);
        consumer = new Consumer(store);
    }

    public String startBatch() {
        return producer.startBatch();
    }

    public void uploadPrices(String batchId, Collection<PriceRecord> records) {
        producer.uploadPrices(batchId, records);
    }

    public void completeBatch(String batchId) {
        producer.completeBatch(batchId);
    }

    public void cancelBatch(String batchId) {
        producer.cancelBatch(batchId);
    }

    public Map<String, PriceRecord> getLastPrices(Collection<String> ids) {
        return consumer.getLastPrices(ids);
    }

    public PriceRecord getLastPrice(String id) {
        return consumer.getLastPrice(id);
    }
}
