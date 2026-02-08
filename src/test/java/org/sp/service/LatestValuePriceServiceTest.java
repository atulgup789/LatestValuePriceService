package org.sp.service;

import org.sp.model.PriceRecord;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LatestValuePriceServiceTest {

    @Test
    void doesNotExposePartialBatch() {
        LatestValuePriceService service = new LatestValuePriceService();
        String batchId = service.startBatch();

        service.uploadPrices(batchId, List.of(
                new PriceRecord("A", Instant.parse("2024-01-01T00:00:00Z"), 100)
        ));

        assertNull(service.getLastPrice("A"));
    }

    @Test
    void completesBatchAtomically() {
        LatestValuePriceService service = new LatestValuePriceService();
        String batchId = service.startBatch();

        service.uploadPrices(batchId, List.of(
                new PriceRecord("A", Instant.parse("2024-01-01T00:00:00Z"), 100),
                new PriceRecord("B", Instant.parse("2024-01-01T00:00:00Z"), 200)
        ));

        service.completeBatch(batchId);

        Map<String, PriceRecord> result = service.getLastPrices(List.of("A", "B"));
        assertEquals(2, result.size());
        assertEquals(100, result.get("A").payload());
        assertEquals(200, result.get("B").payload());
    }

    @Test
    void cancelDiscardsBatch() {
        LatestValuePriceService service = new LatestValuePriceService();
        String batchId = service.startBatch();

        service.uploadPrices(batchId, List.of(
                new PriceRecord("A", Instant.parse("2024-01-01T00:00:00Z"), 100)
        ));

        service.cancelBatch(batchId);

        assertNull(service.getLastPrice("A"));
    }

    @Test
    void lastPriceByAsOfAcrossBatches() {
        LatestValuePriceService service = new LatestValuePriceService();

        String batch1 = service.startBatch();
        service.uploadPrices(batch1, List.of(
                new PriceRecord("A", Instant.parse("2024-01-01T00:00:00Z"), 100)
        ));
        service.completeBatch(batch1);

        String batch2 = service.startBatch();
        service.uploadPrices(batch2, List.of(
                new PriceRecord("A", Instant.parse("2023-12-31T00:00:00Z"), 50)
        ));
        service.completeBatch(batch2);

        assertEquals(100, service.getLastPrice("A").payload());

        String batch3 = service.startBatch();
        service.uploadPrices(batch3, List.of(
                new PriceRecord("A", Instant.parse("2024-02-01T00:00:00Z"), 150)
        ));
        service.completeBatch(batch3);

        assertEquals(150, service.getLastPrice("A").payload());
    }

    @Test
    void toleratesOutOfOrderCalls() {
        LatestValuePriceService service = new LatestValuePriceService();

        //start batch is never called
        service.uploadPrices("missing", List.of(
                new PriceRecord("A", Instant.parse("2024-01-01T00:00:00Z"), 100)
        ));
        service.completeBatch("missing");
        service.cancelBatch("missing");

        assertTrue(service.getLastPrices(List.of("A")).isEmpty());
    }
}
