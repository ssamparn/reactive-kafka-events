package org.reactive.kafka.reactivekafkaproducerconsumer.sec07;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

/*
  goal: to seek offset
*/

public class KafkaConsumer {
    private static final Logger log = LoggerFactory.getLogger(org.reactive.kafka.reactivekafkaproducerconsumer.sec06.KafkaConsumer.class);

    public static void main(String[] args) {
        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );

        var receiverOptions = ReceiverOptions.create(consumerConfig)
                .addAssignListener(consumer -> {
                    consumer.forEach(receiverPartition -> log.info("assigned offset {}", receiverPartition.position())); // position is the offset number
                    consumer.stream()
                            .filter(receiverPartition -> receiverPartition.topicPartition().partition() == 2)
                            .findFirst()
                            .ifPresent(receiverPartition -> receiverPartition.seek(receiverPartition.position() - 2));  // seek value can not be -ve. ensure before setting
                })
                .subscription(List.of("order-events"));

        KafkaReceiver.create(receiverOptions)
                .receive()
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .subscribe();
    }
}
