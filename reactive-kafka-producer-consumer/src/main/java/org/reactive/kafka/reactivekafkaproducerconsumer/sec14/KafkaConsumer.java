package org.reactive.kafka.reactivekafkaproducerconsumer.sec14;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.serializer.ErrorHandlingDeserializer;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.List;
import java.util.Map;

/*
   goal: to demo poison pill messages
*/

public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);

    public static void main(String[] args) {

        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
//                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );

        var options = ReceiverOptions.<String, Integer>create(consumerConfig)
                .withValueDeserializer(errorHandlingDeserializer()) // spring provided error handling deserializer is used
                .subscription(List.of("order-events"));

        KafkaReceiver.create(options)
                .receive()
                .filter(record -> record.value() != -10_000) // Here we are filtering all the failed records. Basically if the event is erroneous then don't process.
                .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))
                .doOnNext(r -> r.receiverOffset().acknowledge())
                .subscribe();

    }

    private static ErrorHandlingDeserializer<Integer> errorHandlingDeserializer(){
        var deserializer = new ErrorHandlingDeserializer<>(new IntegerDeserializer());
        deserializer.setFailedDeserializationFunction(
                info -> {
                    log.error("failed record: {}", new String(info.getData()));
                    return -10_000; // we can provide a fallback / default value to the failed record.
                    // This way the consumer pipeline will not be halted and all the failed records will have a default value.
                }
        );
        return deserializer;
    }

}
