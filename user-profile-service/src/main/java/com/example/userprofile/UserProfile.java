package com.example.userprofile;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrePersist;
import jakarta.persistence.PreUpdate;
import jakarta.persistence.Table;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name = "user_profile")
public class UserProfile {

	@Id
	@Column(name = "user_id", length = 64)
	private String userId;

	@Column(length = 64, unique = true)
	private String account;

	@Column(name = "password_hash", length = 255)
	private String passwordHash;

	@Column(name = "display_name", length = 64)
	private String displayName;

	private Integer age;

	@Column(length = 16)
	private String gender;

	@Column(name = "height_cm", precision = 5, scale = 2)
	private BigDecimal heightCm;

	@Column(name = "weight_kg", precision = 5, scale = 2)
	private BigDecimal weightKg;

	@Column(name = "waist_cm", precision = 5, scale = 2)
	private BigDecimal waistCm;

	@Column(precision = 4, scale = 2)
	private BigDecimal bmi;

	@Column(name = "activity_level", length = 32)
	private String activityLevel;

	@Column(name = "pal_coefficient", precision = 4, scale = 3)
	private BigDecimal palCoefficient;

	@Column(name = "goal_type", length = 16)
	private String goalType;

	@Column(name = "target_weight_kg", precision = 5, scale = 2)
	private BigDecimal targetWeightKg;

	@Column(name = "source_type", length = 32)
	private String sourceType;

	@Column(name = "created_at")
	private LocalDateTime createdAt;

	@Column(name = "updated_at")
	private LocalDateTime updatedAt;

	@PrePersist
	void onCreate() {
		LocalDateTime now = LocalDateTime.now();
		createdAt = now;
		updatedAt = now;
	}

	@PreUpdate
	void onUpdate() {
		updatedAt = LocalDateTime.now();
	}

	public String getUserId() { return userId; }
	public void setUserId(String userId) { this.userId = userId; }
	public String getAccount() { return account; }
	public void setAccount(String account) { this.account = account; }
	public String getPasswordHash() { return passwordHash; }
	public void setPasswordHash(String passwordHash) { this.passwordHash = passwordHash; }
	public String getDisplayName() { return displayName; }
	public void setDisplayName(String displayName) { this.displayName = displayName; }
	public Integer getAge() { return age; }
	public void setAge(Integer age) { this.age = age; }
	public String getGender() { return gender; }
	public void setGender(String gender) { this.gender = gender; }
	public BigDecimal getHeightCm() { return heightCm; }
	public void setHeightCm(BigDecimal heightCm) { this.heightCm = heightCm; }
	public BigDecimal getWeightKg() { return weightKg; }
	public void setWeightKg(BigDecimal weightKg) { this.weightKg = weightKg; }
	public BigDecimal getWaistCm() { return waistCm; }
	public void setWaistCm(BigDecimal waistCm) { this.waistCm = waistCm; }
	public BigDecimal getBmi() { return bmi; }
	public void setBmi(BigDecimal bmi) { this.bmi = bmi; }
	public String getActivityLevel() { return activityLevel; }
	public void setActivityLevel(String activityLevel) { this.activityLevel = activityLevel; }
	public BigDecimal getPalCoefficient() { return palCoefficient; }
	public void setPalCoefficient(BigDecimal palCoefficient) { this.palCoefficient = palCoefficient; }
	public String getGoalType() { return goalType; }
	public void setGoalType(String goalType) { this.goalType = goalType; }
	public BigDecimal getTargetWeightKg() { return targetWeightKg; }
	public void setTargetWeightKg(BigDecimal targetWeightKg) { this.targetWeightKg = targetWeightKg; }
	public String getSourceType() { return sourceType; }
	public void setSourceType(String sourceType) { this.sourceType = sourceType; }
}
