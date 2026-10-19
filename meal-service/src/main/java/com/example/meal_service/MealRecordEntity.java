package com.example.meal_service;

import jakarta.persistence.*;

@Entity
@Table(name = "user_food_nutrition")
public class MealRecordEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String userId;
    private Long intakeId;
    private Long timestamp;
    private String mealType;
    private Double calories;
    private Double sodium;
    private Double potassium;
    @Column(name = "carbohydrate_g")
    private Double carbohydrateG;
    @Column(name = "protein_g")
    private Double proteinG;
    @Column(name = "total_fat_g")
    private Double totalFatG;
    @Column(name = "total_sugar_g")
    private Double totalSugarG;
    @Column(name = "dietary_fiber_g")
    private Double dietaryFiberG;
    @Column(name = "added_sugar_g")
    private Double addedSugarG;
    @Column(name = "saturated_fat_g")
    private Double saturatedFatG;
    @Column(name = "monounsaturated_fat_g")
    private Double monounsaturatedFatG;
    @Column(name = "polyunsaturated_fat_g")
    private Double polyunsaturatedFatG;
    @Column(name = "whole_grain_oz_eq")
    private Double wholeGrainOzEq;
    @Column(name = "refined_grain_oz_eq")
    private Double refinedGrainOzEq;
    @Column(name = "total_vegetable_cup_eq")
    private Double totalVegetableCupEq;
    @Column(name = "dark_green_veg_beans_cup_eq")
    private Double darkGreenVegBeansCupEq;
    @Column(name = "total_fruit_cup_eq")
    private Double totalFruitCupEq;
    @Column(name = "whole_fruit_cup_eq")
    private Double wholeFruitCupEq;
    @Column(name = "dairy_cup_eq")
    private Double dairyCupEq;
    @Column(name = "seafood_plant_protein_oz_eq")
    private Double seafoodPlantProteinOzEq;
    @Column(name = "pattern_data_available")
    private Boolean patternDataAvailable;
    private Double unsaturatedSaturatedFatRatio;

    public MealRecordEntity() {}

    public Long getId() { return id; }
    public void setId(Long id) { this.id = id; }

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public Long getIntakeId() { return intakeId; }
    public void setIntakeId(Long intakeId) { this.intakeId = intakeId; }

    public Long getTimestamp() { return timestamp; }
    public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }

    public String getMealType() { return mealType; }
    public void setMealType(String mealType) { this.mealType = mealType; }

    public Double getCalories() { return calories; }
    public void setCalories(Double calories) { this.calories = calories; }

    public Double getSodium() { return sodium; }
    public void setSodium(Double sodium) { this.sodium = sodium; }

    public Double getPotassium() { return potassium; }
    public void setPotassium(Double potassium) { this.potassium = potassium; }

    public Double getCarbohydrateG() { return carbohydrateG; }
    public void setCarbohydrateG(Double carbohydrateG) { this.carbohydrateG = carbohydrateG; }

    public Double getProteinG() { return proteinG; }
    public void setProteinG(Double proteinG) { this.proteinG = proteinG; }

    public Double getTotalFatG() { return totalFatG; }
    public void setTotalFatG(Double totalFatG) { this.totalFatG = totalFatG; }

    public Double getTotalSugarG() { return totalSugarG; }
    public void setTotalSugarG(Double totalSugarG) { this.totalSugarG = totalSugarG; }

    public Double getDietaryFiberG() { return dietaryFiberG; }
    public void setDietaryFiberG(Double dietaryFiberG) { this.dietaryFiberG = dietaryFiberG; }

    public Double getAddedSugarG() { return addedSugarG; }
    public void setAddedSugarG(Double addedSugarG) { this.addedSugarG = addedSugarG; }

    public Double getSaturatedFatG() { return saturatedFatG; }
    public void setSaturatedFatG(Double saturatedFatG) { this.saturatedFatG = saturatedFatG; }

    public Double getMonounsaturatedFatG() { return monounsaturatedFatG; }
    public void setMonounsaturatedFatG(Double value) { this.monounsaturatedFatG = value; }
    public Double getPolyunsaturatedFatG() { return polyunsaturatedFatG; }
    public void setPolyunsaturatedFatG(Double value) { this.polyunsaturatedFatG = value; }
    public Double getWholeGrainOzEq() { return wholeGrainOzEq; }
    public void setWholeGrainOzEq(Double value) { this.wholeGrainOzEq = value; }
    public Double getRefinedGrainOzEq() { return refinedGrainOzEq; }
    public void setRefinedGrainOzEq(Double value) { this.refinedGrainOzEq = value; }
    public Double getTotalVegetableCupEq() { return totalVegetableCupEq; }
    public void setTotalVegetableCupEq(Double value) { this.totalVegetableCupEq = value; }
    public Double getDarkGreenVegBeansCupEq() { return darkGreenVegBeansCupEq; }
    public void setDarkGreenVegBeansCupEq(Double value) { this.darkGreenVegBeansCupEq = value; }
    public Double getTotalFruitCupEq() { return totalFruitCupEq; }
    public void setTotalFruitCupEq(Double value) { this.totalFruitCupEq = value; }
    public Double getWholeFruitCupEq() { return wholeFruitCupEq; }
    public void setWholeFruitCupEq(Double value) { this.wholeFruitCupEq = value; }
    public Double getDairyCupEq() { return dairyCupEq; }
    public void setDairyCupEq(Double value) { this.dairyCupEq = value; }
    public Double getSeafoodPlantProteinOzEq() { return seafoodPlantProteinOzEq; }
    public void setSeafoodPlantProteinOzEq(Double value) { this.seafoodPlantProteinOzEq = value; }
    public Boolean getPatternDataAvailable() { return patternDataAvailable; }
    public void setPatternDataAvailable(Boolean value) { this.patternDataAvailable = value; }

    public Double getUnsaturatedSaturatedFatRatio() { return unsaturatedSaturatedFatRatio; }
    public void setUnsaturatedSaturatedFatRatio(Double unsaturatedSaturatedFatRatio) { this.unsaturatedSaturatedFatRatio = unsaturatedSaturatedFatRatio; }
}