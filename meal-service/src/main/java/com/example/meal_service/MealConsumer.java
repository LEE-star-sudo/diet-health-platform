package com.example.meal_service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

@Service
public class MealConsumer {

    private static final Logger log = LoggerFactory.getLogger(MealConsumer.class);

    @Autowired
    private FoodIntakeRepository repository;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "user_meal_event_stream", groupId = "meal_mysql_consumer_group", concurrency = "3")
    public void consume(String message) {
        try {
            log.info("Received message from Kafka: {}", message);
            //解析kafka傳進來的消息
            MealEvent event = objectMapper.readValue(message, MealEvent.class);
            if (isBlank(event.getUserId()) || isBlank(event.getMealType())
                    || isBlank(event.getFoodCode()) || isBlank(event.getFoodName())
                    || event.getPortionG() == null || event.getTimestamp() == null) {
                log.error("Skip message missing intake fields: {}", message);
                return;
            }
            //實例化對象
            FoodIntakeEntity entity = new FoodIntakeEntity();
            entity.setUserId(event.getUserId());
            entity.setMealType(event.getMealType());
            entity.setFoodCode(event.getFoodCode());
            entity.setFoodName(event.getFoodName());
            entity.setPortionG(BigDecimal.valueOf(event.getPortionG()).setScale(2, RoundingMode.HALF_UP));
            entity.setRecordTime(LocalDateTime.ofInstant(Instant.ofEpochMilli(event.getTimestamp()), ZoneOffset.UTC));
            //保存到數據庫
            repository.save(entity);
            log.info("Successfully persisted to user_food_intake, record id: {}", entity.getId());

        } catch (Exception e) {
            log.error("Failed to process and persist record: {}", e.getMessage(), e);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
