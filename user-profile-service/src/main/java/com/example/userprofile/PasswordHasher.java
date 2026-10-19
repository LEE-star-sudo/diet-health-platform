package com.example.userprofile;

import java.security.SecureRandom;
import java.util.HexFormat;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;

public final class PasswordHasher {

	private static final SecureRandom RANDOM = new SecureRandom();
	private static final int ITERATIONS = 120_000;

	private PasswordHasher() {
	}

	public static String hash(String password) {
		byte[] salt = new byte[16];
		RANDOM.nextBytes(salt);
		String saltHex = HexFormat.of().formatHex(salt);
		try {
			PBEKeySpec spec = new PBEKeySpec(password.toCharArray(), saltHex.getBytes(), ITERATIONS, 256);
			byte[] digest = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256").generateSecret(spec).getEncoded();
			return "pbkdf2_sha256$" + ITERATIONS + "$" + saltHex + "$" + HexFormat.of().formatHex(digest);
		} catch (Exception exception) {
			throw new IllegalStateException("密码哈希失败", exception);
		}
	}

}
