package org.catamarancode.spring.mvc;

import java.util.Iterator;

import org.catamarancode.util.CommonFieldValidations;
import org.slf4j.Logger;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;
import org.springframework.validation.Errors;
import org.springframework.validation.FieldError;
import org.springframework.validation.ObjectError;

public class SpringValidatorUtils {

    /**
     * For debugging only: Log all validation errors
     */
    public static void logValidationErrors(Logger logger, Errors errors) {
        for (Iterator iter = errors.getAllErrors().iterator(); iter.hasNext();) {
            ObjectError oe = (ObjectError) iter.next();
            if (oe instanceof FieldError) {
                FieldError fe = (FieldError) oe;
                logger
                        .debug("form field '" + fe.getField() + "' code: '"
                                + fe.getCode() + "' message: "
                                + fe.getDefaultMessage());
            } else {
                logger.debug("form object '" + oe.getObjectName() + "' error: "
                        + oe.getDefaultMessage());
            }
        }
    }
    
    private static void assertFieldIsValidString(Errors errors, String field) {
        Assert.notNull(errors, "Errors object must not be null");
        Object value = errors.getFieldValue(field);
        Assert.notNull(value, "Field '" + field + "' must not be null");
        Assert.isInstanceOf(String.class, value, "Field '" + field + "' must be of String type");
    }
    
    public static boolean isEmptyOrWhitespace(Errors errors, String field) {
        assertFieldIsValidString(errors, field);
        Object value = errors.getFieldValue(field);
        if (value == null ||!StringUtils.hasText(value.toString())) {
            return true;
        }
        return false;
    }
    
    public static void rejectIfZero(Errors errors, String field, String errorCode) {
        Assert.notNull(errors, "Errors object must not be null");
        Object value = errors.getFieldValue(field);
        if (value != null && value.toString().equalsIgnoreCase("0")) {
            errors.rejectValue(field, errorCode, null, null);
        }
    }

    
    public static void rejectIfTooLong(Errors errors, String field, String code, int maxLength) {
        assertFieldIsValidString(errors, field);
        Object value = errors.getFieldValue(field);
        if (((String) value).length() > maxLength) {
            errors.rejectValue(field, code);
        }       
    }
    
    public static void rejectIfTooShort(Errors errors, String field, String code, int minLength) {
        assertFieldIsValidString(errors, field);
        Object value = errors.getFieldValue(field);
        if (((String) value).length() < minLength) {
            errors.rejectValue(field, code);
        }       
    }

    public static void rejectInvalidEmail(Errors errors, String field) {
        assertFieldIsValidString(errors, field);
        Object value = errors.getFieldValue(field);
        if (StringUtils.hasText((String) value)) {
            if (!CommonFieldValidations.isValidEmailAddress((String) value)) {
                errors.rejectValue(field, "invalid.email");
            } 
        }
    }
    
    public static void validateEmailConfirm(Errors errors, String emailField, String emailConfirmField) {
        assertFieldIsValidString(errors, emailField);
        assertFieldIsValidString(errors, emailConfirmField);
        String email = (String) errors.getFieldValue(emailField);
        String emailConfirm = (String) errors.getFieldValue(emailConfirmField);
        if (StringUtils.hasText(email) && StringUtils.hasText(emailConfirm)) {
            if (!email.equalsIgnoreCase(emailConfirm)) {
                errors.rejectValue("emailConfirm", "noMatch.emailConfirm");
            }
        }
    }
    
    
    public static void rejectInvalidPassword(Errors errors, String field) {
        assertFieldIsValidString(errors, field);
        Object value = errors.getFieldValue(field);
        String password = (String) value;
        if (StringUtils.hasText(password)) {
            if (password.length() < 6
                    || password.length() > 10) {
                errors.rejectValue(field, "invalid.password");
            }
        }
    }
    
    public static void rejectInvalidPhone(Errors errors, String field) {
        assertFieldIsValidString(errors, field);
        Object value = errors.getFieldValue(field);
        if (!CommonFieldValidations.isValidPhoneNumber((String) value)) {
            errors.rejectValue(field, "invalid.phone");
        }
    }

    public static void rejectInvalidZip(Errors errors, String field) {
        assertFieldIsValidString(errors, field);
        Object value = errors.getFieldValue(field);
        if (!CommonFieldValidations.isValidZipCode((String) value)) {
            errors.rejectValue(field, "invalid.zip");
        }
    }

    private SpringValidatorUtils() {}
}
