package org.reactive.kafka.reactivekafkaproducerconsumer.sec11;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.GroupedFlux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/*
   goal: flatMap - parallel using groupBy
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
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );

        var consumerOptions = ReceiverOptions.<String, String>create(consumerConfig)
                .commitInterval(Duration.ofSeconds(1))
                .subscription(List.of("order-events"));

        KafkaReceiver.create(consumerOptions)
                .receive()
                .groupBy(r -> Integer.parseInt(r.key()) % 5) // just for demo. Here all the events with the same modulus will end up in the same flux / thread.
                // we can also group by r.partition()
                // we can also group by the hashcode of the event key. r.key().hashCode() % 5
                .flatMap(KafkaConsumer::batchProcess) // flatMap() will subscribe to all the publishers at the same time.
                // Its eager subscription. By default, flatMap can subscribe to 256 subscribers at the same time. But there is one major problem with subscribing through flatMap(). That is message ordering.
                // So the solution is to use concatMap() or flatMap() with groupBy if the ordering of the events is important for consumer.
                .subscribe();
    }

    private static Mono<Void> batchProcess(GroupedFlux<Integer, ReceiverRecord<String, String>> flux) {
        return flux
                .publishOn(Schedulers.boundedElastic()) // just for demo
                .doFirst(() -> log.info("---- mod: {}", flux.key()))
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .then(Mono.delay(Duration.ofSeconds(1)))
                .then();
    }
}
