package com.example.meal_service;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface MealRecordRepository extends JpaRepository<MealRecordEntity, Long> {
    boolean existsByIntakeId(Long intakeId);
    Optional<MealRecordEntity> findByIntakeId(Long intakeId);
}