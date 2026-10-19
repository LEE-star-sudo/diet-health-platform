package com.example.userprofile;

import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.server.ResponseStatusException;

import java.math.BigDecimal;
import java.util.UUID;

@Service
public class ProfileService {

	private final UserProfileRepository repository;

	public ProfileService(UserProfileRepository repository) {
		this.repository = repository;
	}

	@Transactional
	public ProfileResponse register(ProfileRequests.RegisterRequest request) {
		validateRequiredText(request.account(), 3, 64, "account");
		validateRequiredText(request.password(), 6, 64, "password");
		validateRequiredText(request.displayName(), 1, 64, "displayName");
		validateAge(request.age());
		validateRange(request.heightCm(), new BigDecimal("50"), new BigDecimal("250"), "heightCm");
		validateRange(request.weightKg(), new BigDecimal("20"), new BigDecimal("300"), "weightKg");
		if (request.waistCm() != null) {
			validateRange(request.waistCm(), new BigDecimal("30"), new BigDecimal("200"), "waistCm");
		}
		if (request.targetWeightKg() != null) {
			validateRange(request.targetWeightKg(), new BigDecimal("20"), new BigDecimal("300"), "targetWeightKg");
		}
		String activityInput = request.activityLevel() == null ? "久坐" : request.activityLevel();
		String goalInput = request.goalType() == null ? "维持" : request.goalType();
		String gender = requiredChoice(BodyMetrics.gender(request.gender()));
		String activity = requiredChoice(BodyMetrics.activity(activityInput));
		String goal = requiredChoice(BodyMetrics.goal(goalInput));
		if (repository.findByAccount(request.account()).isPresent()) {
			throw new ResponseStatusException(HttpStatus.CONFLICT, "账号已存在");
		}

		UserProfile profile = new UserProfile();
		profile.setUserId(UUID.randomUUID().toString().replace("-", "").substring(0, 16));
		profile.setAccount(request.account());
		profile.setPasswordHash(PasswordHasher.hash(request.password()));
		profile.setDisplayName(request.displayName());
		profile.setAge(request.age());
		profile.setGender(gender);
		profile.setHeightCm(request.heightCm());
		profile.setWeightKg(request.weightKg());
		profile.setWaistCm(request.waistCm());
		profile.setBmi(BodyMetrics.bmi(request.weightKg(), request.heightCm()));
		profile.setActivityLevel(activity);
		profile.setPalCoefficient(BodyMetrics.pal(activity));
		profile.setGoalType(goal);
		profile.setTargetWeightKg(request.targetWeightKg() == null ? request.weightKg() : request.targetWeightKg());
		profile.setSourceType("real");
		return ProfileResponse.from(repository.save(profile));
	}

	@Transactional(readOnly = true)
	public ProfileResponse get(String userId) {
		return ProfileResponse.from(requireRealUser(userId));
	}

	@Transactional
	public ProfileResponse update(String userId, ProfileRequests.UpdateRequest request) {
		UserProfile profile = requireRealUser(userId);
		if (request.displayName() != null) {
			validateRequiredText(request.displayName(), 1, 64, "displayName");
			profile.setDisplayName(request.displayName());
		}
		if (request.age() != null) {
			validateAge(request.age());
			profile.setAge(request.age());
		}
		if (request.gender() != null) {
			profile.setGender(requiredChoice(BodyMetrics.gender(request.gender())));
		}
		if (request.heightCm() != null) {
			validateRange(request.heightCm(), new BigDecimal("50"), new BigDecimal("250"), "heightCm");
			profile.setHeightCm(request.heightCm());
		}
		if (request.weightKg() != null) {
			validateRange(request.weightKg(), new BigDecimal("20"), new BigDecimal("300"), "weightKg");
			profile.setWeightKg(request.weightKg());
		}
		if (request.waistCm() != null) {
			validateRange(request.waistCm(), new BigDecimal("30"), new BigDecimal("200"), "waistCm");
			profile.setWaistCm(request.waistCm());
		}
		if (request.activityLevel() != null) {
			String activity = requiredChoice(BodyMetrics.activity(request.activityLevel()));
			profile.setActivityLevel(activity);
			profile.setPalCoefficient(BodyMetrics.pal(activity));
		}
		if (request.goalType() != null) {
			profile.setGoalType(requiredChoice(BodyMetrics.goal(request.goalType())));
		}
		if (request.targetWeightKg() != null) {
			validateRange(request.targetWeightKg(), new BigDecimal("20"), new BigDecimal("300"), "targetWeightKg");
			profile.setTargetWeightKg(request.targetWeightKg());
		}
		profile.setBmi(BodyMetrics.bmi(profile.getWeightKg(), profile.getHeightCm()));
		return ProfileResponse.from(repository.save(profile));
	}

	private UserProfile requireRealUser(String userId) {
		UserProfile profile = repository.findById(userId)
				.filter(item -> "real".equals(item.getSourceType()) && item.getAccount() != null)
				.orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "用户不存在"));
		return profile;
	}

	private static String requiredChoice(String value) {
		if (value == null) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "gender、activityLevel 或 goalType 无法识别");
		}
		return value;
	}

	private static void validateRequiredText(String value, int min, int max, String field) {
		if (value == null || value.length() < min || value.length() > max) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, field + " 长度不符合要求");
		}
	}

	private static void validateAge(Integer age) {
		if (age == null || age < 18 || age > 120) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "age 必须在 18 到 120 之间");
		}
	}

	private static void validateRange(BigDecimal value, BigDecimal min, BigDecimal max, String field) {
		if (value.compareTo(min) <= 0 || value.compareTo(max) >= 0) {
			throw new ResponseStatusException(HttpStatus.BAD_REQUEST, field + " 超出允许范围");
		}
	}

}
