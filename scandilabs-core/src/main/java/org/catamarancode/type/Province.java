package org.catamarancode.type;


public enum Province {
    AB(Country.CANADA, "Alberta"),
    BC(Country.CANADA, "British Columbia"),
    MB(Country.CANADA, "Manitoba"),
    NB(Country.CANADA, "New Brunswick"),
    NL(Country.CANADA, "Newfoundland"),
    NT(Country.CANADA, "Northwest Territories"),
    NS(Country.CANADA, "Nova Scotia"),
    NU(Country.CANADA, "Nunavut"),
    ON(Country.CANADA, "Ontario"),
    PE(Country.CANADA, "Prince Edward Island"),
    QC(Country.CANADA, "Quebec"),
    SK(Country.CANADA, "Saskatchewan"),
    YT(Country.CANADA, "Yukon Territory");

private String longName;
private Country country;

private Province(Country country, String longName) {
    this.country = country;
    setLongName(longName);
}

public String getLongName() {
    return longName;
}

private void setLongName(String longName) {
    this.longName = longName;
}

public Country getCountry() {
    return country;
}

public static Province[] getByCountry(Country country) {
    return null;
}
}