package com.example.meal_service;

import jakarta.persistence.*;
import java.math.BigDecimal;

@Entity
@Table(name = "food_nutrition_dict")
public class FoodNutritionEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String foodCode;

    @Column(name = "unit_weight_g", precision = 6, scale = 2)
    private BigDecimal unitWeightG;

    @Column(name = "energy_kcal", precision = 6, scale = 2)
    private BigDecimal energyKcal;

    @Column(name = "carbohydrate_g", precision = 6, scale = 2)
    private BigDecimal carbohydrateG;

    @Column(name = "protein_g", precision = 6, scale = 2)
    private BigDecimal proteinG;

    @Column(name = "total_fat_g", precision = 6, scale = 2)
    private BigDecimal totalFatG;

    @Column(name = "total_sugar_g", precision = 6, scale = 2)
    private BigDecimal totalSugarG;

    @Column(name = "dietary_fiber_g", precision = 6, scale = 2)
    private BigDecimal dietaryFiberG;

    @Column(name = "saturated_fat_g", precision = 6, scale = 2)
    private BigDecimal saturatedFatG;

    @Column(name = "monounsaturated_fat_g", precision = 6, scale = 2)
    private BigDecimal monounsaturatedFatG;

    @Column(name = "polyunsaturated_fat_g", precision = 6, scale = 2)
    private BigDecimal polyunsaturatedFatG;

    @Column(name = "sodium_mg", precision = 7, scale = 2)
    private BigDecimal sodiumMg;

    @Column(name = "potassium_mg", precision = 7, scale = 2)
    private BigDecimal potassiumMg;

    @Column(name = "added_sugar_g", precision = 8, scale = 2)
    private BigDecimal addedSugarG;

    @Column(name = "whole_grain_oz_eq", precision = 10, scale = 4)
    private BigDecimal wholeGrainOzEq;

    @Column(name = "refined_grain_oz_eq", precision = 10, scale = 4)
    private BigDecimal refinedGrainOzEq;

    @Column(name = "total_vegetable_cup_eq", precision = 10, scale = 4)
    private BigDecimal totalVegetableCupEq;

    @Column(name = "dark_green_veg_beans_cup_eq", precision = 10, scale = 4)
    private BigDecimal darkGreenVegBeansCupEq;

    @Column(name = "total_fruit_cup_eq", precision = 10, scale = 4)
    private BigDecimal totalFruitCupEq;

    @Column(name = "whole_fruit_cup_eq", precision = 10, scale = 4)
    private BigDecimal wholeFruitCupEq;

    @Column(name = "dairy_cup_eq", precision = 10, scale = 4)
    private BigDecimal dairyCupEq;

    @Column(name = "seafood_plant_protein_oz_eq", precision = 10, scale = 4)
    private BigDecimal seafoodPlantProteinOzEq;

    @Column(name = "fped_source")
    private String fpedSource;

    public Long getId() { return id; }
    public String getFoodCode() { return foodCode; }
    public BigDecimal getUnitWeightG() { return unitWeightG; }
    public BigDecimal getEnergyKcal() { return energyKcal; }
    public BigDecimal getCarbohydrateG() { return carbohydrateG; }
    public BigDecimal getProteinG() { return proteinG; }
    public BigDecimal getTotalFatG() { return totalFatG; }
    public BigDecimal getTotalSugarG() { return totalSugarG; }
    public BigDecimal getDietaryFiberG() { return dietaryFiberG; }
    public BigDecimal getSaturatedFatG() { return saturatedFatG; }
    public BigDecimal getMonounsaturatedFatG() { return monounsaturatedFatG; }
    public BigDecimal getPolyunsaturatedFatG() { return polyunsaturatedFatG; }
    public BigDecimal getSodiumMg() { return sodiumMg; }
    public BigDecimal getPotassiumMg() { return potassiumMg; }
    public BigDecimal getAddedSugarG() { return addedSugarG; }
    public BigDecimal getWholeGrainOzEq() { return wholeGrainOzEq; }
    public BigDecimal getRefinedGrainOzEq() { return refinedGrainOzEq; }
    public BigDecimal getTotalVegetableCupEq() { return totalVegetableCupEq; }
    public BigDecimal getDarkGreenVegBeansCupEq() { return darkGreenVegBeansCupEq; }
    public BigDecimal getTotalFruitCupEq() { return totalFruitCupEq; }
    public BigDecimal getWholeFruitCupEq() { return wholeFruitCupEq; }
    public BigDecimal getDairyCupEq() { return dairyCupEq; }
    public BigDecimal getSeafoodPlantProteinOzEq() { return seafoodPlantProteinOzEq; }
    public String getFpedSource() { return fpedSource; }
}
