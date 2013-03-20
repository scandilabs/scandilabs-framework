package com.scandilabs.framework.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Framework independent validation utils. Code that depends on Spring should be
 * put elsewhere.
 * 
 * @author mkvalsvik
 * 
 */
public class CommonFieldValidations {

    private CommonFieldValidations() {
    }

    private static final Pattern ZIP_CODE = Pattern
            .compile("^(\\d{5}(-\\d{4})?)$");

    private static final Pattern EMAIL_PATTERN = Pattern
            .compile("^([a-zA-Z0-9_\\.\\'\\-\\+])+\\@(([a-zA-Z0-9\\-])+\\.)+([a-zA-Z]{2,4})+$");

    // Between 3 and 8 char length. First char must be alphanumeric.
    // The rest must be alphanumeric or dash (-)
    private static final Pattern COMPANY_SHORTNAME_PATTERN = Pattern
            .compile("^[a-z0-9]([a-z0-9\\-]{2,7})$");

    // US telephone number with area code. Validates and also captures AreaCode,
    // Prefix and Suffix for reformatting.
    // Matches: (213) 343-1234 | 213-343-1234 | 213 343 1234
    // Area code must start with 2 or higher
    private static final Pattern PHONE_NUMBER_PATTERN = Pattern
            .compile("^\\(?([2-9]\\d{2})(\\)?)(-|.|\\s)?([1-9]\\d{2})(-|.|\\s)?(\\d{4})$");

    /**
     * Checks whether <code>number</code> is a valid an properly formatted US
     * phone number. Does not ensure that the area code is an existing one.
     * 
     * @param number
     *            the phone number to check.
     * @return true if <code>number</code> is in a valid format, false if not.
     */
    public static boolean isValidPhoneNumber(String number) {
        Matcher matcher = PHONE_NUMBER_PATTERN.matcher(number);
        ;
        return matcher.find();
    }

    /**
     * Matches: 12345, 12345-1234 Fails: 12345-12345
     * 
     * @param zip
     * @return
     */
    public static boolean isValidZipCode(String zip) {
        Matcher matcher = ZIP_CODE.matcher(zip);
        ;
        return matcher.find();
    }

    /**
     * Checks whether <code>email</code> is properly formatted e-mail address.
     * (This only checks the format, and does not check whether this is an
     * actual or active e-mail account.)
     * 
     * @param email
     *            the e-mail address to check.
     * @return true if <code>email</code> is in a valid format, false if not.
     */
    public static boolean isValidEmailAddress(String email) {
        Matcher matcher = EMAIL_PATTERN.matcher(email);
        return matcher.find();
    }

    /**
     * Checks whether <code>name</code> is properly formatted for a company
     * short name (First char alphanumeric. All others alphanumeric or dash)
     * 
     * @param name
     * @return
     */
    public static boolean isValidCompanyShortName(String name) {
        Matcher matcher = COMPANY_SHORTNAME_PATTERN.matcher(name);
        return matcher.find();
    }

}
