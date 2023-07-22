package org.reactive.kafka.reactivekafkawithspring.config.producer;

import org.reactive.kafka.reactivekafkawithspring.model.OrderEvent;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import reactor.kafka.sender.SenderOptions;

@Configuration
public class ReactiveKafkaProducerConfig {

    @Bean
    public SenderOptions<String, OrderEvent> senderOptions(KafkaProperties properties) {
        return SenderOptions.create(properties.buildProducerProperties());
    }

    @Bean
    public ReactiveKafkaProducerTemplate<String, OrderEvent> producerTemplate(SenderOptions<String, OrderEvent> senderOptions) {
        return new ReactiveKafkaProducerTemplate<>(senderOptions);
    }

}
