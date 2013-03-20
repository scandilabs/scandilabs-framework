package com.scandilabs.framework.util;

import com.scandilabs.framework.util.CommonFieldValidations;

import junit.framework.TestCase;

public class CommonFieldValidationsTest extends TestCase {
	
	public void testIsValidCompanyShortName() {
		assertTrue(CommonFieldValidations.isValidCompanyShortName("test"));
		assertTrue(CommonFieldValidations.isValidCompanyShortName("test-"));
		assertTrue(CommonFieldValidations.isValidCompanyShortName("test-ing"));
		assertTrue(CommonFieldValidations.isValidCompanyShortName("test123"));
		assertTrue(CommonFieldValidations.isValidCompanyShortName("0test"));
		assertTrue(CommonFieldValidations.isValidCompanyShortName("tes"));
		assertTrue(CommonFieldValidations.isValidCompanyShortName("t--es"));

		
		assertFalse(CommonFieldValidations.isValidCompanyShortName("-test"));
		assertFalse(CommonFieldValidations.isValidCompanyShortName(".test"));
		assertFalse(CommonFieldValidations.isValidCompanyShortName("te.st"));
		assertFalse(CommonFieldValidations.isValidCompanyShortName("test ing"));
		assertFalse(CommonFieldValidations.isValidCompanyShortName("testingggg"));
		assertFalse(CommonFieldValidations.isValidCompanyShortName("te"));
		assertFalse(CommonFieldValidations.isValidCompanyShortName("test#ing"));
		assertFalse(CommonFieldValidations.isValidCompanyShortName("test%"));
		assertFalse(CommonFieldValidations.isValidCompanyShortName("*test"));
		assertFalse(CommonFieldValidations.isValidCompanyShortName("te^st"));
	}
	
	public void testIsValidPhoneNumber() {
		assertTrue(CommonFieldValidations.isValidPhoneNumber("212-123-1234"));
		assertTrue(CommonFieldValidations.isValidPhoneNumber("223 123 1234"));
		assertTrue(CommonFieldValidations.isValidPhoneNumber("(223) 123-1234"));
		assertTrue(CommonFieldValidations.isValidPhoneNumber("(223)123-1234"));
		
		assertFalse(CommonFieldValidations.isValidPhoneNumber("-test"));
		assertFalse(CommonFieldValidations.isValidPhoneNumber("123123123"));
		assertFalse(CommonFieldValidations.isValidPhoneNumber("112-123-1234"));
		assertFalse(CommonFieldValidations.isValidPhoneNumber("112-123-123"));
	}
	public void testIsValidZip() {
		assertTrue(CommonFieldValidations.isValidZipCode("12345"));
		assertTrue(CommonFieldValidations.isValidZipCode("12345-1234"));
		assertFalse(CommonFieldValidations.isValidZipCode("12345-12345"));
		assertFalse(CommonFieldValidations.isValidZipCode("1234"));
	}
}
