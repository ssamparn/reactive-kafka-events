package org.reactive.kafka.reactivekafkaproducerconsumer.sec05;

public class KafkaConsumerGroup {

    private static class Consumer1 {
        public static void main(String[] args) {
            KafkaConsumer.start("1");
        }
    }

    private static class Consumer2 {
        public static void main(String[] args) {
            KafkaConsumer.start("2");
        }
    }

    private static class Consumer3 {
        public static void main(String[] args) {
            KafkaConsumer.start("3");
        }
    }

}
