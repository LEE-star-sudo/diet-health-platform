package com.example.meal_service;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class NutritionTopicConfig {

    @Bean
    public NewTopic nutritionEventTopic() {
        return new NewTopic(NutritionConversionService.NUTRITION_TOPIC, 3, (short) 1);
    }
}
