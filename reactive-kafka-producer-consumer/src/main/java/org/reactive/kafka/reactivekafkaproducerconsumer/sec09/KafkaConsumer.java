package org.reactive.kafka.reactivekafkaproducerconsumer.sec09;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/*
   goal: receiveAutoAck with concatMap
*/
public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    public static void main(String[] args) {
        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092", // Using single node kafka
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 3 // This is the config which is responsible for consuming the events in batches.
                // If there are 100 events, then 34 flux will be created.
                // Default value for MAX_POLL_RECORDS_CONFIG is 500
        );

        var receiverOptions = ReceiverOptions.create(consumerConfig)
                .commitInterval(Duration.ofSeconds(1)) // When kafka consumer commits to the broker about acknowledgement, it happens in batches in a periodic manner. Default commit interval is 5 seconds.
                .subscription(List.of("order-events"));

        KafkaReceiver.create(receiverOptions)
                .receiveAutoAck()
                .log()
                .concatMap(KafkaConsumer::batchProcess)
                .subscribe();
    }

    private static Mono<Void> batchProcess(Flux<ConsumerRecord<Object, Object>> consumerRecordFlux) {
        return consumerRecordFlux
                .doFirst(() -> log.info("---- Flux Started ----"))
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))
                .then(Mono.delay(Duration.ofSeconds(1)))
                .then();

    }

}
