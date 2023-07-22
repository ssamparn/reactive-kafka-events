package org.reactive.kafka.reactivekafkawithspring.embeddedkafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.test.condition.EmbeddedKafkaCondition;
import org.springframework.kafka.test.context.EmbeddedKafka;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.Map;


@EmbeddedKafka(
//        ports = 9092, // Making embedded kafka run on random port.
        partitions = 1,
        brokerProperties = { "auto.create.topics.enable=false" },
        topics = { "order-events" }
)
public class EmbeddedKafkaTest {

    @Test
    public void embeddedKafkaTest() {
        var brokers = EmbeddedKafkaCondition.getBroker().getBrokersAsString(); // 127.0.0.1:51136

        StepVerifier.create(Producer.run(brokers))
                .verifyComplete();

        StepVerifier.create(Consumer.run(brokers))
                .verifyComplete();
    }

    private static class Producer {
        private static final Logger log = LoggerFactory.getLogger(Producer.class);
        public static Mono<Void> run(String brokers) {
            var producerConfig = Map.<String, Object>of(
                    ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers, // here instead of localhost we are providing the embedded kafka broker server property.
                    ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                    ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
            );

            var options = SenderOptions.<String, String>create(producerConfig);

            var messageFLux = Flux.range(1, 10)
                    .delayElements(Duration.ofMillis(10))
                    .map(i -> new ProducerRecord<>("order-events", i.toString(), "order-" + i))
                    .map(pr -> SenderRecord.create(pr, pr.key()));

            var sender = KafkaSender.create(options);

            return sender.send(messageFLux)
                    .doOnNext(r -> log.info("correlation id: {}", r.correlationMetadata()))
                    .doOnComplete(sender::close)
                    .then();
        }
    }

    private static class Consumer {
        private static final Logger log = LoggerFactory.getLogger(Consumer.class);
        public static Mono<Void> run(String brokers) {

            var consumerConfig = Map.<String, Object>of(
                    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers,
                    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                    ConsumerConfig.GROUP_ID_CONFIG, "demo-group-123",
                    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                    ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
            );

            var options = ReceiverOptions.create(consumerConfig)
                    .subscription(List.of("order-events"));

            return KafkaReceiver.create(options)
                    .receive()
                    .take(10)
                    .doOnNext(r -> log.info("key: {}, value: {}", r.key(), r.value()))
                    .doOnNext(r -> r.receiverOffset().acknowledge())
                    .then();
        }
    }
}
