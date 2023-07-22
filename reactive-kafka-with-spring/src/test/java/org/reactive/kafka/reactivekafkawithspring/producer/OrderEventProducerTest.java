package org.reactive.kafka.reactivekafkawithspring.producer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.reactive.kafka.reactivekafkawithspring.AbstractIntegrationTest;
import org.reactive.kafka.reactivekafkawithspring.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.annotation.DirtiesContext;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.test.StepVerifier;

import java.time.Duration;

public class OrderEventProducerTest extends AbstractIntegrationTest {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducerTest.class);

    @Test
    @DirtiesContext(methodMode = DirtiesContext.MethodMode.AFTER_METHOD)
    public void producerTest1() {
        KafkaReceiver<String, OrderEvent> receiver = createReceiver("order-events");
        var orderEvents = receiver.receive()
                .take(10)
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()));

        StepVerifier.create(orderEvents)
                .consumeNextWith(r -> Assertions.assertNotNull(r.value().orderId()))
                .expectNextCount(9)
                .expectComplete()
                .verify(Duration.ofSeconds(10));
    }

}
