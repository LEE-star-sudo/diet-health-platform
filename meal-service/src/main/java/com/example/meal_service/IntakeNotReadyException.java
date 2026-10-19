package com.example.meal_service;

public class IntakeNotReadyException extends RuntimeException {
    public IntakeNotReadyException(String message) {
        super(message);
    }
}
