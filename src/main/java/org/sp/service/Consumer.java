package org.sp.service;

import org.sp.model.PriceRecord;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

final class Consumer {
    private final Store store;

    public Consumer(Store store) {
        this.store = Objects.requireNonNull(store, "store");
    }

    public Map<String, PriceRecord> getLastPrices(Collection<String> ids) {
        // Reads from a stable snapshot to avoid partial visibility.
        Map<String, PriceRecord> snapshot = store.snapshot();
        Map<String, PriceRecord> result = new HashMap<>(ids.size());
        for (String id : ids) {
            if (id == null) {
                continue;
            }
            PriceRecord record = snapshot.get(id);
            if (record != null) {
                result.put(id, record);
            }
        }
        return Collections.unmodifiableMap(result);
    }

    public PriceRecord getLastPrice(String id) {
        return store.snapshot().get(id);
    }
}
