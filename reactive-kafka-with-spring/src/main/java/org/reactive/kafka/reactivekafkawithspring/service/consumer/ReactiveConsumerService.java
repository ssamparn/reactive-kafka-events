package org.reactive.kafka.reactivekafkawithspring.service.consumer;

import org.reactive.kafka.reactivekafkawithspring.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Service;

@Service
public class ReactiveConsumerService implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(ReactiveConsumerService.class);

    private final ReactiveKafkaConsumerTemplate<String, OrderEvent> consumerTemplate;

    @Autowired
    public ReactiveConsumerService(ReactiveKafkaConsumerTemplate<String, OrderEvent> consumerTemplate) {
        this.consumerTemplate = consumerTemplate;
    }

    @Override
    public void run(String... args) {
        this.consumerTemplate.receive()
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))
                .doOnNext(r -> r.headers().forEach(h -> log.info("header key: {}, value: {}", h.key(), new String(h.value()))))
                .doOnError(throwable -> log.error("something bad happened while consuming : {}", throwable.getMessage()))
                .subscribe();
    }
}
