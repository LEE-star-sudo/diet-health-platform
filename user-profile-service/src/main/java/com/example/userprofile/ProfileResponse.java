package com.example.userprofile;

import java.math.BigDecimal;

public record ProfileResponse(
		String userId,
		String account,
		String displayName,
		String sourceType,
		Integer age,
		String ageBand,
		String gender,
		BigDecimal heightCm,
		BigDecimal weightKg,
		BigDecimal waistCm,
		BigDecimal bmi,
		String bmiLevel,
		Boolean waistRisk,
		String activityLevel,
		BigDecimal palCoefficient,
		String goalType,
		BigDecimal targetWeightKg,
		BigDecimal bmr,
		BigDecimal tdeeKcal
) {

	public static ProfileResponse from(UserProfile profile) {
		return new ProfileResponse(
				profile.getUserId(),
				profile.getAccount(),
				profile.getDisplayName(),
				profile.getSourceType(),
				profile.getAge(),
				BodyMetrics.ageBand(profile.getAge()),
				profile.getGender(),
				profile.getHeightCm(),
				profile.getWeightKg(),
				profile.getWaistCm(),
				profile.getBmi(),
				profile.getBmi() == null ? null : BodyMetrics.bmiLevel(profile.getBmi()),
				BodyMetrics.waistRisk(profile.getGender(), profile.getWaistCm()),
				profile.getActivityLevel(),
				profile.getPalCoefficient(),
				profile.getGoalType(),
				profile.getTargetWeightKg(),
				BodyMetrics.bmr(profile.getGender(), profile.getWeightKg(), profile.getHeightCm(), profile.getAge()),
				BodyMetrics.tdee(profile.getGender(), profile.getWeightKg(), profile.getHeightCm(), profile.getAge(), profile.getActivityLevel())
		);
	}

}
