package com.example.userprofile;

import java.math.BigDecimal;

public final class ProfileRequests {

	private ProfileRequests() {
	}

	public record RegisterRequest(
			String account,
			String password,
			String displayName,
			Integer age,
			String gender,
			BigDecimal heightCm,
			BigDecimal weightKg,
			BigDecimal waistCm,
			String activityLevel,
			String goalType,
			BigDecimal targetWeightKg
	) {
	}

	public record UpdateRequest(
			String displayName,
			Integer age,
			String gender,
			BigDecimal heightCm,
			BigDecimal weightKg,
			BigDecimal waistCm,
			String activityLevel,
			String goalType,
			BigDecimal targetWeightKg
	) {
	}

}
