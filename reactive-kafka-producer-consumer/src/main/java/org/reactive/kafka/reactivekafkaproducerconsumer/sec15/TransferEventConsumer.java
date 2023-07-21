package org.reactive.kafka.reactivekafkaproducerconsumer.sec15;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverRecord;

public class TransferEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(TransferEventConsumer.class);
    private final KafkaReceiver<String, String> kafkaReceiver;

    public TransferEventConsumer(KafkaReceiver<String, String> kafkaReceiver) {
        this.kafkaReceiver = kafkaReceiver;
    }

    public Flux<TransferEvent> receiveTransferEvent() {
        return this.kafkaReceiver.receive()
                .doOnNext(record -> log.info("key: {}, value: {}", record.key(), record.value()))
                .map(this::toTransferEvent);
    }


    // source event: > key: from, to, amount

    // target event: > key: from - amount
    //               > key: to + amount

    // If the key is 5, then validation fails and reject the transaction.
    // If the key is 6, then acknowledgement failure.
    private TransferEvent toTransferEvent(ReceiverRecord<String, String> receiverRecord) {
        String[] event = receiverRecord.value().split(",");
        var runnable = receiverRecord.key().equals("6") ? fail() : ack(receiverRecord);

        return new TransferEvent(
                receiverRecord.key(),
                event[0],
                event[1],
                event[2],
                runnable
        );
    }

    private Runnable ack(ReceiverRecord<String, String> record){
        return () -> record.receiverOffset().acknowledge();
    }

    private Runnable fail(){
        return () -> { throw new RuntimeException("error while ack");  };
    }
}
