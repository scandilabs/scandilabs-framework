package com.scandilabs.framework.type;

import java.util.regex.Pattern;

/**
 * @see http://www.unc.edu/~rowlett/units/codes/country.htm
 * @author mkvalsvik
 *
 */
public enum Country {

    USA(Pattern.compile("^[0-9]{5}$"), "840", "US"),
        CANADA(Pattern.compile("^([a-zA-Z][0-9]){3}$"), "124", "CA"),
        PUERTO_RICO(Pattern.compile("^[0-9]{5}$"), "630", "PR"),
    	JAPAN(null, "392", "JP"),
    	MEXICO(null, "484", "MX");

    private final Pattern zipCodePattern;
    private final String isoCode;
    private final String isoTwoLetterCode;

    private Country(Pattern pattern, String isoCode, String isoTwoLetterCode) {
        this.zipCodePattern = pattern;
        this.isoCode = isoCode;
        this.isoTwoLetterCode = isoTwoLetterCode;
    }

    public boolean isZipValid(String zipCode) {
        return zipCodePattern.matcher(zipCode).matches();
    }

    public String getIsoCode() {
        return isoCode;
    }
    
    public String getIsoTwoLetterCode() {
    	return isoTwoLetterCode;
    }

    public static Country getCountryWithIsoCode(String isoCode) {
        for (Country country : Country.values()) {
            if (country.getIsoCode().equals(isoCode)) {
                return country;
            }
        }
        return null;
    }
}
