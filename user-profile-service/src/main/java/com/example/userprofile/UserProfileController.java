package com.example.userprofile;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api")
public class UserProfileController {

	private final ProfileService profileService;

	public UserProfileController(ProfileService profileService) {
		this.profileService = profileService;
	}

	@GetMapping("/health")
	public Map<String, String> health() {
		return Map.of("status", "ok");
	}

	@PostMapping("/users/register")
	@ResponseStatus(HttpStatus.CREATED)
	public ProfileResponse register(@RequestBody ProfileRequests.RegisterRequest request) {
		return profileService.register(request);
	}

	@GetMapping("/users/{userId}")
	public ProfileResponse get(@PathVariable String userId) {
		return profileService.get(userId);
	}

	@PutMapping("/users/{userId}")
	public ProfileResponse update(@PathVariable String userId, @RequestBody ProfileRequests.UpdateRequest request) {
		return profileService.update(userId, request);
	}

}
