package com.scandilabs.framework.util;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.commons.codec.binary.Base64;

public class PasswordUtils {

	private PasswordUtils() {}

	public static boolean passwordMatches(String cleartextPassword, String encodedPassword, String applicationSalt) {
		return passwordMatches(cleartextPassword, encodedPassword, applicationSalt, null);
	}
	
	/**
	 * Encodes cleartext password and compares it with the encoded one
	 * 
	 * @param cleartextPassword
	 * @return true if match, false otherwise
	 */
	public static boolean passwordMatches(String cleartextPassword, String encodedPassword, String applicationSalt, String userSalt) {
		if (encodedPassword == null) {
			return false;
		}
		String encodedCleartextPassword = encode(cleartextPassword, applicationSalt, userSalt);
		return encodedPassword.equals(encodedCleartextPassword);
	}
	
	public static String encode(String password, String applicationSalt) {
		return encode(password, applicationSalt, null);
	}
	
	/**
	 * Encodes password using SHA-1 and base64.
	 * 
	 * @param password
	 *            the cleartext password
	 * @return the encoded password as a String
	 */
	public static String encode(String password, String applicationSalt, String userSalt) {
		StringBuffer sb = new StringBuffer();
		
		// User-specific salt is recommended (we suggest using the ID/PK of an object) but optional
		if (userSalt != null) {
			sb.append(userSalt);	
		}
		
		sb.append(applicationSalt);
		sb.append(password);
		MessageDigest md = null;
		try {
			md = MessageDigest.getInstance("SHA-1");
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException(
					"Password encoding algorithm SHA-1 not found, please check you Java version",
					e);
		}
		try {
			md.update(sb.toString().getBytes("UTF-8"));
			byte[] hash = md.digest();
			Base64 base64Encoder = new Base64();
			byte[] base64EncodedHash = base64Encoder.encode(hash);
			return new String(base64EncodedHash, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(
					"UTF-8 not supported, please check you Java version", e);
		}
	}

}
