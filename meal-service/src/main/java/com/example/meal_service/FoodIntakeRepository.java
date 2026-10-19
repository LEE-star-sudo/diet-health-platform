package com.example.meal_service;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

@Repository
public interface FoodIntakeRepository extends JpaRepository<FoodIntakeEntity, Long> {
    List<FoodIntakeEntity> findByUserIdAndRecordTimeAndFoodCodeAndPortionGOrderByIdAsc(
            String userId, LocalDateTime recordTime, String foodCode, BigDecimal portionG);
}
