package org.reactive.kafka.reactivekafkaproducerconsumer.sec15;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderRecord;
import reactor.kafka.sender.SenderResult;

import java.time.Duration;
import java.util.function.Predicate;

public class TransferEventProcessor {

    private static final Logger log = LoggerFactory.getLogger(TransferEventProcessor.class);
    private final KafkaSender<String, String> kafkaSender;

    public TransferEventProcessor(KafkaSender<String, String> kafkaSender) {
        this.kafkaSender = kafkaSender;
    }

    // source event: > key: from, to, amount

    // target event: > key: from - amount
    //               > key: to + amount

    // If the key is 5, then validation fails and reject the transaction.
    // If the key is 6, then acknowledgement failure.

    public Flux<SenderResult<String>> processTransferEvent(Flux<TransferEvent> transferEventFlux) {
        return transferEventFlux
                .concatMap(this::validateTransaction)
                .concatMap(this::sendTransaction);
    }

    // As per the requirement, if the key is 5, then the account does not have enough money to transfer
    private Mono<TransferEvent> validateTransaction(TransferEvent transferEvent) {
        return Mono.just(transferEvent)
                .filter(Predicate.not(event -> event.key().equals("5"))) // If it is not 5, then proceed further.
                .switchIfEmpty(
                        Mono.<TransferEvent>fromRunnable(transferEvent.acknowledge()) // So in case the key is 5, we will get an empty mono, but then we will have to switch the mono to not take further action and acknowledge the event.
                                .doFirst(() -> log.info("transfer validation fails for key: {}", transferEvent.key()))
                );
    }

    private Flux<SenderRecord<String, String, String>> toSenderRecords(TransferEvent transferEvent) {
        ProducerRecord creditRecord =new ProducerRecord<>("transaction-events", transferEvent.key(), "%s+%s".formatted(transferEvent.to(), transferEvent.amount()));
        ProducerRecord debitRecord = new ProducerRecord<>("transaction-events", transferEvent.key(), "%s-%s".formatted(transferEvent.from(), transferEvent.amount()));

        var sr1 = SenderRecord.create(creditRecord, creditRecord.key());
        var sr2 = SenderRecord.create(debitRecord, debitRecord.key());

        return Flux.just(sr1, sr2);
    }

    private Mono<SenderResult<String>> sendTransaction(TransferEvent transferEvent) {
        var senderRecords = this.toSenderRecords(transferEvent);

        var transactionManager = this.kafkaSender.transactionManager();

        return transactionManager.begin()
                .then(this.kafkaSender.send(senderRecords)
                        .concatWith(Mono.delay(Duration.ofSeconds(1))
                                .then(Mono.fromRunnable(transferEvent.acknowledge()))) // delaying the acknowledgement by 1 second.
                        .concatWith(transactionManager.commit())
                        .last())
                .doOnError(ex -> log.error(ex.getMessage()))
                .onErrorResume(ex -> transactionManager.abort());
    }
}
