package com.example.meal_service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.List;

@Service
public class NutritionConversionService {

    public static final String NUTRITION_TOPIC = "user_food_nutrition_stream";
    private static final int INTAKE_LOOKUP_ATTEMPTS = 10;
    private static final long INTAKE_LOOKUP_WAIT_MS = 200;

    private final FoodNutritionRepository foodNutritionRepository;
    private final FoodIntakeRepository foodIntakeRepository;
    private final MealRecordRepository mealRecordRepository;
    private final NutritionOutboxRepository outboxRepository;
    private final ObjectMapper objectMapper;

    public NutritionConversionService(
            FoodNutritionRepository foodNutritionRepository,
            FoodIntakeRepository foodIntakeRepository,
            MealRecordRepository mealRecordRepository,
            NutritionOutboxRepository outboxRepository) {
        this.foodNutritionRepository = foodNutritionRepository;
        this.foodIntakeRepository = foodIntakeRepository;
        this.mealRecordRepository = mealRecordRepository;
        this.outboxRepository = outboxRepository;
        this.objectMapper = new ObjectMapper();
    }

    @Transactional
    public MealRecordEntity convertAndQueue(MealEvent event) throws InterruptedException, JsonProcessingException {
        FoodNutritionEntity food = foodNutritionRepository.findByFoodCode(event.getFoodCode())
                .orElseThrow(() -> new IllegalArgumentException(
                        "food_code not in dictionary: " + event.getFoodCode()));
        if (food.getUnitWeightG() == null || food.getUnitWeightG().signum() == 0) {
            throw new IllegalArgumentException("unit weight missing for food_code: " + event.getFoodCode());
        }

        BigDecimal portionG = BigDecimal.valueOf(event.getPortionG()).setScale(2, RoundingMode.HALF_UP);
        LocalDateTime recordTime = LocalDateTime.ofInstant(
                Instant.ofEpochMilli(event.getTimestamp()), ZoneOffset.UTC);
        FoodIntakeEntity intake = findIntake(event.getUserId(), recordTime, event.getFoodCode(), portionG);
        if (intake == null) {
            throw new IntakeNotReadyException("matching user_food_intake row not found");
        }

        MealRecordEntity nutrition = mealRecordRepository.findByIntakeId(intake.getId())
                .orElseGet(() -> createNutrition(event, intake, food, portionG));
        if (!outboxRepository.existsByNutritionId(nutrition.getId())) {
            NutritionOutboxEntity outbox = new NutritionOutboxEntity();
            outbox.setNutritionId(nutrition.getId());
            outbox.setEventKey(event.getUserId());
            outbox.setTopic(NUTRITION_TOPIC);
            outbox.setPayload(toPayload(event, nutrition));
            outboxRepository.save(outbox);
        }
        return nutrition;
    }

    private MealRecordEntity createNutrition(
            MealEvent event, FoodIntakeEntity intake, FoodNutritionEntity food, BigDecimal portionG) {
        MealRecordEntity entity = new MealRecordEntity();
        entity.setUserId(event.getUserId());
        entity.setIntakeId(intake.getId());
        entity.setTimestamp(event.getTimestamp());
        entity.setMealType(event.getMealType());
        entity.setCalories(scale(food.getEnergyKcal(), portionG, food.getUnitWeightG()));
        entity.setCarbohydrateG(scale(food.getCarbohydrateG(), portionG, food.getUnitWeightG()));
        entity.setProteinG(scale(food.getProteinG(), portionG, food.getUnitWeightG()));
        entity.setTotalFatG(scale(food.getTotalFatG(), portionG, food.getUnitWeightG()));
        entity.setTotalSugarG(scale(food.getTotalSugarG(), portionG, food.getUnitWeightG()));
        entity.setDietaryFiberG(scale(food.getDietaryFiberG(), portionG, food.getUnitWeightG()));
        entity.setSaturatedFatG(scale(food.getSaturatedFatG(), portionG, food.getUnitWeightG()));
        entity.setMonounsaturatedFatG(scale(food.getMonounsaturatedFatG(), portionG, food.getUnitWeightG()));
        entity.setPolyunsaturatedFatG(scale(food.getPolyunsaturatedFatG(), portionG, food.getUnitWeightG()));
        entity.setSodium(scale(food.getSodiumMg(), portionG, food.getUnitWeightG()));
        entity.setPotassium(scale(food.getPotassiumMg(), portionG, food.getUnitWeightG()));
        entity.setUnsaturatedSaturatedFatRatio(fatRatio(food));

        boolean patternAvailable = food.getFpedSource() != null
                && !"missing".equals(food.getFpedSource())
                && food.getAddedSugarG() != null;
        entity.setPatternDataAvailable(patternAvailable);
        if (patternAvailable) {
            entity.setAddedSugarG(scale(food.getAddedSugarG(), portionG, food.getUnitWeightG()));
            entity.setWholeGrainOzEq(scale(food.getWholeGrainOzEq(), portionG, food.getUnitWeightG()));
            entity.setRefinedGrainOzEq(scale(food.getRefinedGrainOzEq(), portionG, food.getUnitWeightG()));
            entity.setTotalVegetableCupEq(scale(food.getTotalVegetableCupEq(), portionG, food.getUnitWeightG()));
            entity.setDarkGreenVegBeansCupEq(scale(food.getDarkGreenVegBeansCupEq(), portionG, food.getUnitWeightG()));
            entity.setTotalFruitCupEq(scale(food.getTotalFruitCupEq(), portionG, food.getUnitWeightG()));
            entity.setWholeFruitCupEq(scale(food.getWholeFruitCupEq(), portionG, food.getUnitWeightG()));
            entity.setDairyCupEq(scale(food.getDairyCupEq(), portionG, food.getUnitWeightG()));
            entity.setSeafoodPlantProteinOzEq(scale(
                    food.getSeafoodPlantProteinOzEq(), portionG, food.getUnitWeightG()));
        }
        return mealRecordRepository.saveAndFlush(entity);
    }

    private FoodIntakeEntity findIntake(
            String userId, LocalDateTime recordTime, String foodCode, BigDecimal portionG)
            throws InterruptedException {
        for (int attempt = 1; attempt <= INTAKE_LOOKUP_ATTEMPTS; attempt++) {
            List<FoodIntakeEntity> rows = foodIntakeRepository
                    .findByUserIdAndRecordTimeAndFoodCodeAndPortionGOrderByIdAsc(
                            userId, recordTime, foodCode, portionG);
            if (!rows.isEmpty()) {
                return rows.get(0);
            }
            if (attempt < INTAKE_LOOKUP_ATTEMPTS) {
                Thread.sleep(INTAKE_LOOKUP_WAIT_MS);
            }
        }
        return null;
    }

    private String toPayload(MealEvent event, MealRecordEntity value) throws JsonProcessingException {
        ObjectNode json = objectMapper.createObjectNode();
        json.put("nutritionId", value.getId());
        json.put("intakeId", value.getIntakeId());
        json.put("userId", value.getUserId());
        json.put("timestamp", value.getTimestamp());
        json.put("mealType", value.getMealType());
        json.put("foodCode", event.getFoodCode());
        json.put("portionG", event.getPortionG());
        put(json, "calories", value.getCalories());
        put(json, "carbohydrateG", value.getCarbohydrateG());
        put(json, "proteinG", value.getProteinG());
        put(json, "totalFatG", value.getTotalFatG());
        put(json, "totalSugarG", value.getTotalSugarG());
        put(json, "dietaryFiberG", value.getDietaryFiberG());
        put(json, "saturatedFatG", value.getSaturatedFatG());
        put(json, "monounsaturatedFatG", value.getMonounsaturatedFatG());
        put(json, "polyunsaturatedFatG", value.getPolyunsaturatedFatG());
        put(json, "sodiumMg", value.getSodium());
        put(json, "potassiumMg", value.getPotassium());
        json.put("patternDataAvailable", Boolean.TRUE.equals(value.getPatternDataAvailable()));
        put(json, "addedSugarG", value.getAddedSugarG());
        put(json, "wholeGrainOzEq", value.getWholeGrainOzEq());
        put(json, "refinedGrainOzEq", value.getRefinedGrainOzEq());
        put(json, "totalVegetableCupEq", value.getTotalVegetableCupEq());
        put(json, "darkGreenVegBeansCupEq", value.getDarkGreenVegBeansCupEq());
        put(json, "totalFruitCupEq", value.getTotalFruitCupEq());
        put(json, "wholeFruitCupEq", value.getWholeFruitCupEq());
        put(json, "dairyCupEq", value.getDairyCupEq());
        put(json, "seafoodPlantProteinOzEq", value.getSeafoodPlantProteinOzEq());
        return objectMapper.writeValueAsString(json);
    }

    private static void put(ObjectNode node, String name, Double value) {
        if (value == null) {
            node.putNull(name);
        } else {
            node.put(name, value);
        }
    }

    private static Double fatRatio(FoodNutritionEntity food) {
        BigDecimal saturated = zeroIfNull(food.getSaturatedFatG());
        if (saturated.signum() == 0) {
            return null;
        }
        return zeroIfNull(food.getMonounsaturatedFatG())
                .add(zeroIfNull(food.getPolyunsaturatedFatG()))
                .divide(saturated, 2, RoundingMode.HALF_UP)
                .doubleValue();
    }

    private static BigDecimal zeroIfNull(BigDecimal value) {
        return value == null ? BigDecimal.ZERO : value;
    }

    private static double scale(BigDecimal perUnit, BigDecimal portionG, BigDecimal unitWeightG) {
        return zeroIfNull(perUnit).multiply(portionG)
                .divide(unitWeightG, 4, RoundingMode.HALF_UP)
                .doubleValue();
    }
}
