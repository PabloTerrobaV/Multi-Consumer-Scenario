package com.example.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import com.example.kafka.Order;

import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
@EnableKafka
@ComponentScan(basePackages = "com.example.kafka.controllers") // Incluye el paquete del controlador
public class OrderConsumer1 {

    public static void main(String[] args) {
        SpringApplication.run(OrderConsumer1.class, args);
    }

    @Bean
    public ConsumerFactory<String, Order> consumerFactory() {
        Map<String, Object> props = new HashMap<>();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://localhost:9092");
        // props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://172.18.0.5:9092");
        // props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://host.docker.internal:9092");
        // props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "http://kafka:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "store-orders-consumer-group");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "io.confluent.kafka.serializers.KafkaAvroDeserializer");
        props.put("schema.registry.url", "http://localhost:8081");
        // props.put("schema.registry.url", "http://schema-registry:8081"); // Para utilizar las imñagenes de Docker
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put("specific.avro.reader", true); // Habilita la deserialización a objetos Avro específicos
        return new DefaultKafkaConsumerFactory<>(props);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Order> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Order> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }

    @KafkaListener(topics = "store-orders", groupId = "store-orders-consumer-group")
    public void listen(Order order) {
        System.out.println("📥 Received order:");
        System.out.println(order);
        System.out.println("-------------------------------------");
    }
}