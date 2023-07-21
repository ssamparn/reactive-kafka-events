package org.reactive.kafka.reactivekafkaproducerconsumer.sec15;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;

import java.util.List;
import java.util.Map;

public class TransferDemo {

    private static final Logger log = LoggerFactory.getLogger(TransferDemo.class);

    public static void main(String[] args) {
        TransferEventConsumer transferEventConsumer = new TransferEventConsumer(createKafkaConsumer());
        TransferEventProcessor transferEventProcessor = new TransferEventProcessor(createKafkaProducer());

        transferEventConsumer
                .receiveTransferEvent()
                .transform(transferEventProcessor::processTransferEvent)
                .doOnNext(r -> log.info("transfer success: {} ", r.correlationMetadata()))
                .doOnError(ex -> log.error(ex.getMessage()))
                .subscribe();
    }

    private static KafkaReceiver<String, String> createKafkaConsumer(){
        var consumerConfig = Map.<String, Object>of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.GROUP_ID_CONFIG, "demo-group",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, "1"
        );
        var receiverOptions = ReceiverOptions.<String, String>create(consumerConfig)
                .subscription(List.of("transfer-requests")); // transfer events will be received in this topic.
                                                                // transfer event will look like: > key: from, to, amount

        return KafkaReceiver.create(receiverOptions);
    }

    private static KafkaSender<String, String> createKafkaProducer(){
        var producerConfig = Map.<String, Object>of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.TRANSACTIONAL_ID_CONFIG, "money-transfer" // since we will be using kafka transaction manager at the producer side, we need to have this property TRANSACTIONAL_ID_CONFIG.
                // V. Imp Note: TRANSACTIONAL_ID_CONFIG property has to be unique for each instance of producer. Otherwise, the producer will get ProducerFencedException.
        );
        var options = SenderOptions.<String, String>create(producerConfig);
        return KafkaSender.create(options);
    }

}
