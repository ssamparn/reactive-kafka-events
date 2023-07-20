package org.reactive.kafka.reactivekafkaproducerconsumer.sec09;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.util.Map;

/*
   goal: receiveAutoAck with concatMap

*/
public class KafkaProducer {
    private static final Logger log = LoggerFactory.getLogger(KafkaProducer.class);

    public static void main(String[] args) {

        var producerConfig = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092", // Using single node kafka
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class
        );

        var producerOptions = SenderOptions.<String, String>create(producerConfig);

        var messageFlux = Flux.range(1, 100)
                .map(i -> new ProducerRecord<>("order-events", i.toString(), "order-"+i))
                .map(pr -> SenderRecord.create(pr, pr.key()));

        var sender = KafkaSender.create(producerOptions);
        sender.send(messageFlux)
                .doOnNext(r -> log.info("correlation id: {}", r.correlationMetadata()))
                .doOnComplete(sender::close)
                .subscribe();
    }
}
