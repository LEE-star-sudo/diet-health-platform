package com.example.meal_service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
public class NutritionOutboxPublisher {

    private static final Logger log = LoggerFactory.getLogger(NutritionOutboxPublisher.class);
    private static final List<String> RETRYABLE_STATUSES = List.of("PENDING", "FAILED");

    private final NutritionOutboxRepository outboxRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public NutritionOutboxPublisher(
            NutritionOutboxRepository outboxRepository,
            KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxRepository = outboxRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelayString = "${nutrition.outbox.publish-delay-ms:1000}")
    @Transactional
    public void publishPending() {
        List<NutritionOutboxEntity> batch =
                outboxRepository.findTop100ByStatusInOrderByIdAsc(RETRYABLE_STATUSES);
        for (NutritionOutboxEntity event : batch) {
            event.setAttempts(event.getAttempts() + 1);
            try {
                kafkaTemplate.send(
                                event.getTopic(),
                                event.getEventKey(),
                                event.getPayload())
                        .get(10, TimeUnit.SECONDS);
                event.setStatus("SENT");
                event.setSentAt(Instant.now());
                event.setLastError(null);
            } catch (Exception exception) {
                event.setStatus("FAILED");
                event.setLastError(abbreviate(exception.getMessage()));
                log.error("Outbox event {} publish failed on attempt {}",
                        event.getId(), event.getAttempts(), exception);
            }
        }
        outboxRepository.saveAll(batch);
    }

    private static String abbreviate(String value) {
        if (value == null) {
            return "unknown publish error";
        }
        return value.length() <= 1000 ? value : value.substring(0, 1000);
    }
}
