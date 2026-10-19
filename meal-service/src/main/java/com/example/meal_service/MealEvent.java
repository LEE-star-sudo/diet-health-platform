package com.example.meal_service;

import java.io.Serializable;

public class MealEvent implements Serializable {
    private String userId;
    private Long timestamp;
    private String mealType;
    private String foodCode;
    private String foodName;
    private Double portionG;

    public MealEvent() {}

    public String getUserId() { return userId; }
    public void setUserId(String userId) { this.userId = userId; }

    public Long getTimestamp() { return timestamp; }
    public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }

    public String getMealType() { return mealType; }
    public void setMealType(String mealType) { this.mealType = mealType; }

    public String getFoodCode() { return foodCode; }
    public void setFoodCode(String foodCode) { this.foodCode = foodCode; }

    public String getFoodName() { return foodName; }
    public void setFoodName(String foodName) { this.foodName = foodName; }

    public Double getPortionG() { return portionG; }
    public void setPortionG(Double portionG) { this.portionG = portionG; }
}
