package com.example.meal_service;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class NutritionConsumer {

    private static final Logger log = LoggerFactory.getLogger(NutritionConsumer.class);

    @Autowired
    private NutritionConversionService conversionService;

    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(
            topics = "user_meal_event_stream",
            groupId = "meal_nutrition_consumer_group",
            concurrency = "3",
            containerFactory = "nutritionKafkaListenerContainerFactory")
    public void consume(String message) {
        try {
            //解析kafka傳進來的消息
            MealEvent event = objectMapper.readValue(message, MealEvent.class);
            if (isBlank(event.getUserId()) || isBlank(event.getMealType())
                    || isBlank(event.getFoodCode()) || isBlank(event.getFoodName())
                    || event.getPortionG() == null || event.getTimestamp() == null) {
                log.error("Skip nutrition message missing intake fields: {}", message);
                return;
            }
            //實例化對象然後換算
            MealRecordEntity entity = conversionService.convertAndQueue(event);
            log.info("Successfully persisted to user_food_nutrition, record id: {}, intake id: {}",
                    entity.getId(), entity.getIntakeId());
        } catch (IntakeNotReadyException e) {
            throw e;
        } catch (IllegalArgumentException e) {
            log.error("Skip nutrition message: {}", e.getMessage());
        } catch (Exception e) {
            log.error("Failed to convert intake into nutrition: {}", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
