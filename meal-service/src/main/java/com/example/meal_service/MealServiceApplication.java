package com.example.meal_service;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class MealServiceApplication {

	public static void main(String[] args) {
		SpringApplication.run(MealServiceApplication.class, args);
	}

}
