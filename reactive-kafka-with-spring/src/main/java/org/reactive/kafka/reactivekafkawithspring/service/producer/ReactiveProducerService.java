package org.reactive.kafka.reactivekafkawithspring.service.producer;

import org.reactive.kafka.reactivekafkawithspring.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.UUID;

@Service
public class ReactiveProducerService implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(ReactiveProducerService.class);
    private final ReactiveKafkaProducerTemplate<String, OrderEvent> producerTemplate;

    @Autowired
    public ReactiveProducerService(ReactiveKafkaProducerTemplate<String, OrderEvent> producerTemplate) {
        this.producerTemplate = producerTemplate;
    }

    @Override
    public void run(String... args) {
        this.orderEventFlux()
                .flatMap(orderEvent -> this.producerTemplate.send("order-events", orderEvent.orderId().toString(), orderEvent))
                .doOnNext(result -> log.info("result: {}", result.recordMetadata()))
                .subscribe();
    }

    private Flux<OrderEvent> orderEventFlux() {
        return Flux.interval(Duration.ofMillis(500))
                .take(50)
                .map(i -> new OrderEvent(
                        UUID.randomUUID(),
                        i,
                        LocalDateTime.now()
                ));
    }
}
