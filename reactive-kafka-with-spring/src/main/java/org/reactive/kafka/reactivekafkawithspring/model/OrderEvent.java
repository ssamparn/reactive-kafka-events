package org.reactive.kafka.reactivekafkawithspring.model;

import java.time.LocalDateTime;
import java.util.UUID;

public record OrderEvent(
        UUID orderId,
        long customerId,
        LocalDateTime orderDate
) {
}
