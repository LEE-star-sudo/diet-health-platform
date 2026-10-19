package com.example.userprofile;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;

public final class BodyMetrics {

	private static final Map<String, String> ACTIVITIES = Map.ofEntries(
			Map.entry("sedentary", "sedentary"),
			Map.entry("久坐", "sedentary"),
			Map.entry("light", "light"),
			Map.entry("轻度", "light"),
			Map.entry("轻度活动", "light"),
			Map.entry("moderate", "moderate"),
			Map.entry("中度", "moderate"),
			Map.entry("中度活动", "moderate")
	);

	private static final Map<String, BigDecimal> PAL = Map.of(
			"sedentary", new BigDecimal("1.200"),
			"light", new BigDecimal("1.375"),
			"moderate", new BigDecimal("1.550")
	);

	private static final Map<String, String> GOALS = Map.of(
			"lose", "lose",
			"减重", "lose",
			"maintain", "maintain",
			"维持", "maintain",
			"gain", "gain",
			"增重", "gain"
	);

	private BodyMetrics() {
	}

	public static String gender(String value) {
		if (value == null) {
			return null;
		}
		return switch (value.trim().toLowerCase()) {
			case "m", "male", "男", "1" -> "Male";
			case "f", "female", "女", "2" -> "Female";
			default -> null;
		};
	}

	public static String activity(String value) {
		if (value == null) {
			return null;
		}
		String direct = ACTIVITIES.get(value.trim());
		return direct != null ? direct : ACTIVITIES.get(value.trim().toLowerCase());
	}

	public static String goal(String value) {
		if (value == null) {
			return null;
		}
		String direct = GOALS.get(value.trim());
		return direct != null ? direct : GOALS.get(value.trim().toLowerCase());
	}

	public static BigDecimal pal(String activity) {
		return PAL.get(activity);
	}

	public static BigDecimal bmi(BigDecimal weightKg, BigDecimal heightCm) {
		BigDecimal meters = heightCm.divide(new BigDecimal("100"), 6, RoundingMode.HALF_UP);
		return weightKg.divide(meters.multiply(meters), 2, RoundingMode.HALF_UP);
	}

	public static String bmiLevel(BigDecimal bmi) {
		if (bmi.compareTo(new BigDecimal("18.5")) < 0) {
			return "偏瘦";
		}
		if (bmi.compareTo(new BigDecimal("23.9")) <= 0) {
			return "正常";
		}
		if (bmi.compareTo(new BigDecimal("27.9")) <= 0) {
			return "超重";
		}
		return "肥胖";
	}

	public static String ageBand(int age) {
		if (age <= 29) {
			return "18-29";
		}
		if (age <= 49) {
			return "30-49";
		}
		return "50+";
	}

	public static BigDecimal bmr(String gender, BigDecimal weightKg, BigDecimal heightCm, int age) {
		BigDecimal base = weightKg.multiply(BigDecimal.TEN)
				.add(heightCm.multiply(new BigDecimal("6.25")))
				.subtract(BigDecimal.valueOf(age).multiply(new BigDecimal("5")));
		BigDecimal adjustment = "Male".equals(gender) ? new BigDecimal("5") : new BigDecimal("-161");
		return base.add(adjustment).setScale(2, RoundingMode.HALF_UP);
	}

	public static BigDecimal tdee(String gender, BigDecimal weightKg, BigDecimal heightCm, int age, String activity) {
		return bmr(gender, weightKg, heightCm, age).multiply(pal(activity)).setScale(2, RoundingMode.HALF_UP);
	}

	public static Boolean waistRisk(String gender, BigDecimal waistCm) {
		if (waistCm == null) {
			return null;
		}
		BigDecimal limit = "Male".equals(gender) ? new BigDecimal("90") : new BigDecimal("85");
		return waistCm.compareTo(limit) >= 0;
	}

}
