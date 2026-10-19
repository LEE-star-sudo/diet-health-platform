package com.example.userprofile;

import org.springframework.data.jpa.repository.JpaRepository;

import java.util.Optional;

public interface UserProfileRepository extends JpaRepository<UserProfile, String> {

	Optional<UserProfile> findByAccount(String account);

}
