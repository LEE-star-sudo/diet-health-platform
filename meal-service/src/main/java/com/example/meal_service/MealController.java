package com.example.meal_service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/meal")
public class MealController {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    private final ObjectMapper objectMapper = new ObjectMapper();

    private static final String TOPIC_NAME = "user_meal_event_stream";

    @PostMapping("/record")
    public ResponseEntity<String> recordMeal(@RequestBody MealEvent event) {
        if (isBlank(event.getUserId())) {
            return ResponseEntity.badRequest().body("userId 不能为空");
        }
        if (isBlank(event.getMealType())) {
            return ResponseEntity.badRequest().body("mealType 不能为空");
        }
        if (isBlank(event.getFoodCode())) {
            return ResponseEntity.badRequest().body("foodCode 不能为空");
        }
        if (isBlank(event.getFoodName())) {
            return ResponseEntity.badRequest().body("foodName 不能为空");
        }
        if (event.getPortionG() == null || event.getPortionG() <= 0) {
            return ResponseEntity.badRequest().body("portionG 必须大于 0");
        }
        if (event.getTimestamp() == null) {
            event.setTimestamp(System.currentTimeMillis());
        }

        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("userId", event.getUserId());
        payload.put("timestamp", event.getTimestamp());
        payload.put("mealType", event.getMealType());
        payload.put("foodCode", event.getFoodCode());
        payload.put("foodName", event.getFoodName());
        payload.put("portionG", event.getPortionG());

        kafkaTemplate.send(TOPIC_NAME, event.getUserId(), payload.toString());
        return ResponseEntity.ok("Success: 饮食数据已成功投递到 Kafka");
    }

    private static boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
