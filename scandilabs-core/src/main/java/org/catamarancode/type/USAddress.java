package org.catamarancode.type;

import javax.persistence.Column;
import javax.persistence.Embeddable;


@Embeddable
public class USAddress {

	private String street1;
	private String street2;
	private String city;
	private String zipCode;
	private State state;
	private double latitude;
	private double longitude;

	@Column(length = 100)
	public String getStreet1() {
		return street1;
	}

	public void setStreet1(String _street) {
		this.street1 = _street;
	}

	@Column(length = 100)
	public String getStreet2() {
		return street2;
	}

	public void setStreet2(String _street) {
		this.street2 = _street;
	}

	@Column(length = 100)
	public String getCity() {
		return city;
	}

	public void setCity(String _city) {
		this.city = _city;
	}

	@Column(length = 5)
	public String getZipCode() {
		return zipCode;
	}

	public void setZipCode(String code) {
		this.zipCode = code;
	}

	@Column(name = "STATE")
	public State getState() {
		return state;
	}

	public void setState(State _state) {
		this.state = _state;
	}

	public String toString() {
		return "{street1=" + street1 + ",street2=" + street2 + ",city=" + city
				+ ",zipCode=" + zipCode + ",state=" + state + "}";
	}
	
	public String asOneLine() {
		if (street2 != null) {
			return street1 + ", " + street2 + ", " + city + ", " + state + " " + zipCode;
		}
		return street1 + ", " + city + ", " + state + " " + zipCode;
		
	}

    public double getLatitude() {
        return latitude;
    }

    public void setLatitude(double latitude) {
        this.latitude = latitude;
    }

    public double getLongitude() {
        return longitude;
    }

    public void setLongitude(double longitude) {
        this.longitude = longitude;
    }
}
