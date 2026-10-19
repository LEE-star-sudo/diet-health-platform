package com.example.meal_service;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface FoodNutritionRepository extends JpaRepository<FoodNutritionEntity, Long> {
    Optional<FoodNutritionEntity> findByFoodCode(String foodCode);
}
