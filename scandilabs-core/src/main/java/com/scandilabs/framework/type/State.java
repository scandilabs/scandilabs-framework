package com.scandilabs.framework.type;


public enum State {
    AL(Country.USA, "Alabama"),
    AK(Country.USA, "Alaska"),
    AZ(Country.USA, "Arizona"),
    AR(Country.USA, "Arkansas"),
    CA(Country.USA, "California"),
    CO(Country.USA, "Colorado"),
    CT(Country.USA, "Connecticut"),
    DE(Country.USA, "Delaware"),
    DC(Country.USA, "District of Columbia"),
    FL(Country.USA, "Florida"),
    GA(Country.USA, "Georgia"),
    HI(Country.USA, "Hawaii"),
    ID(Country.USA, "Idaho"),
    IL(Country.USA, "Illinois"),
    IN(Country.USA, "Indiana"),
    IA(Country.USA, "Iowa"),
    KS(Country.USA, "Kansas"),
    KY(Country.USA, "Kentucky"),
    LA(Country.USA, "Louisiana"),
    ME(Country.USA, "Maine"),
    MD(Country.USA, "Maryland"),
    MA(Country.USA, "Massachusetts"),
    MI(Country.USA, "Michigan"),
    MN(Country.USA, "Minnesota"),
    MS(Country.USA, "Mississippi"),
    MO(Country.USA, "Missouri"),
    MT(Country.USA, "Montana"),
    NE(Country.USA, "Nebraska"),
    NV(Country.USA, "Nevada"),
    NH(Country.USA, "New Hampshire"),
    NJ(Country.USA, "New Jersey"),
    NM(Country.USA, "New Mexico"),
    NY(Country.USA, "New York"),
    NC(Country.USA, "North Carolina"),
    ND(Country.USA, "North Dakota"),
    OH(Country.USA, "Ohio"),
    OK(Country.USA, "Oklahoma"),
    OR(Country.USA, "Oregon"),
    PA(Country.USA, "Pennsylvania"),
    RI(Country.USA, "Rhode Island"),
    SC(Country.USA, "South Carolina"),
    SD(Country.USA, "South Dakota"),
    TN(Country.USA, "Tennessee"),
    TX(Country.USA, "Texas"),
    UT(Country.USA, "Utah"),
    VT(Country.USA, "Vermont"),
    VA(Country.USA, "Virginia"),
    WA(Country.USA, "Washington"),
    WV(Country.USA, "West Virginia"),
    WI(Country.USA, "Wisconsin"),
    WY(Country.USA, "Wyoming");

private String longName;
private Country country;

private State(Country country, String longName) {
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

public static State[] getByCountry(Country country) {
    return null;
}
}