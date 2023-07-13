package org.reactive.kafka.reactivekafkaproducerconsumer.sec04;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.Map;
import java.util.UUID;

/*
 goal: to produce records along with headers
 */
public class KafkaProducer {

    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    public static void main(String[] args) {
        var producerConfig = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        var senderOptions = SenderOptions.<String, String>create(producerConfig);

        var messageFlux = Flux.range(1, 10)
                .map(KafkaProducer::createSenderRecord);

        var sender = KafkaSender.create(senderOptions);
        sender.send(messageFlux)
                .doOnNext(r -> log.info("correlation id: {}", r.correlationMetadata()))
                .doOnComplete(sender::close)
                .subscribe();
    }

    private static SenderRecord<String, String, String> createSenderRecord(Integer i) {
        var requestHeaders = new RecordHeaders();
        requestHeaders.add("X-Request-Id", UUID.randomUUID().toString().getBytes());
        requestHeaders.add("Trace-Id", UUID.randomUUID().toString().getBytes());

        var producerRecord = new ProducerRecord<>("order-events", null, i.toString(), "order-" + i, requestHeaders);

        return SenderRecord.create(producerRecord, producerRecord.key());
    }

}
