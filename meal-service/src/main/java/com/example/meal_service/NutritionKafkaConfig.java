package com.example.meal_service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class NutritionKafkaConfig {

    private static final Logger log = LoggerFactory.getLogger(NutritionKafkaConfig.class);

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> nutritionKafkaListenerContainerFactory(
            ConsumerFactory<String, String> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(new DefaultErrorHandler(
                (ConsumerRecord<?, ?> record, Exception exception) -> log.error(
                        "Give up nutrition conversion after retries: {}", record.value(), exception),
                new FixedBackOff(1000L, 20L)));
        return factory;
    }
}
