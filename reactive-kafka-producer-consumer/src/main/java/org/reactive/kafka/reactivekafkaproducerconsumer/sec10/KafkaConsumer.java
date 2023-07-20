package org.reactive.kafka.reactivekafkaproducerconsumer.sec10;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/*
    goal: receiveAutoAck with flatMap - parallel
*/
public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    public static void main(String[] args) {

        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3
        );

        var consumerOptions = ReceiverOptions.create(consumerConfig)
                .commitInterval(Duration.ofSeconds(1))
                .subscription(List.of("order-events"));

        KafkaReceiver.create(consumerOptions)
                .receiveAutoAck()
                .log()
                .flatMap(KafkaConsumer::batchProcess) // flatMap() will subscribe to all the publishers at the same time.
                // Its eager subscription. By default, flatMap can subscribe to 256 subscribers at the same time. But this can be configured using a concurrency factor.
                .subscribe();
    }

    private static Mono<Void> batchProcess(Flux<ConsumerRecord<Object, Object>> flux){
        return flux
                .publishOn(Schedulers.boundedElastic()) // This is just for demo. In real life scenarios, we might have to make a database operation or call any external APIs.
                // We can do those operation(s) as part of a separate thread pool.
                .doFirst(() -> log.info("---- Flux Started ----"))
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))
                .then(Mono.delay(Duration.ofSeconds(1)))
                .then();
    }
}
