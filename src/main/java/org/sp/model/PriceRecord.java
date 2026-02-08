package org.sp.model;

import java.time.Instant;
import java.util.Objects;

/**
 * Immutable price record produced by a batch.
 */
public record PriceRecord(String id, Instant asOf, Object payload) {
    public PriceRecord(String id, Instant asOf, Object payload) {
        this.id = Objects.requireNonNull(id, "id");
        this.asOf = Objects.requireNonNull(asOf, "asOf");
        this.payload = payload;
    }
}
