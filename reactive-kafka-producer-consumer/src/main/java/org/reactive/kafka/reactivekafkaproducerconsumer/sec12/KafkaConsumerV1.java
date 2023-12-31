package org.reactive.kafka.reactivekafkaproducerconsumer.sec12;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.util.retry.Retry;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/*
   error handling demo: a simple processing issue with retry. Receiver and Processor in the same pipeline.
*/
public class KafkaConsumerV1 {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumerV1.class);

    public static void main(String[] args) {

        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );

        var options = ReceiverOptions.create(consumerConfig)
                .subscription(List.of("order-events"));

        KafkaReceiver.create(options)
                .receive()
                .log()
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value().toString().toCharArray()[15])) // just for demo we are creating an error situation.
                // After the error, a cancel signal will be sent upstream and error signal will be sent downstream.
                .doOnError(ex -> log.error(ex.getMessage()))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .retryWhen(Retry.fixedDelay(3, Duration.ofSeconds(1))) // Even after retrying, the consumer will retry but will not be able to recover from the error situation.
                // Ideally this should be handled in a separate pipeline. See the implementation of KafkaConsumerV2.
//                .subscribe(); // for production
                .blockLast(); // just for demo
    }

}
