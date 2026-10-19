package com.example.meal_service;

import jakarta.persistence.LockModeType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;

import java.util.Collection;
import java.util.List;

public interface NutritionOutboxRepository extends JpaRepository<NutritionOutboxEntity, Long> {
    boolean existsByNutritionId(Long nutritionId);
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    List<NutritionOutboxEntity> findTop100ByStatusInOrderByIdAsc(Collection<String> statuses);
}
