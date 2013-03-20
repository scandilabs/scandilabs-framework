package com.scandilabs.framework.type;

import javax.persistence.Column;
import javax.persistence.Embeddable;

import org.apache.commons.lang.StringUtils;

@Embeddable
public class Name {

    private String first;

    private String middle;

    private String last;

    @Column(length = 50)
    public String getFirst() {
        return first;
    }

    public void setFirst(String _first) {
        this.first = _first;
    }

    @Column(length = 50)
    public String getMiddle() {
        return middle;
    }

    public void setMiddle(String _middle) {
        this.middle = _middle;
    }

    @Column(length = 50)
    public String getLast() {
        return last;
    }

    public void setLast(String _last) {
        this.last = _last;
    }

    public String toString() {
        return asFullName();
    }

    public String asFullName() {
        String middleStr = middle == null ? "" : middle + " ";
        return first + " " + middleStr + last;
    }

    /**
     * Builds a Name object from a full name like "first last" or "last, first"
     * 
     * @param fullName
     * @return
     */
    public static Name createFromFullNameString(String fullName) {

        if (fullName == null) {
            return null;
        }
        String firstName = null;
        String lastName = null;
        int commaPos = fullName.indexOf(',');
        int spacePos = fullName.indexOf(' ');
        if (commaPos > 0) {
            lastName = fullName.substring(0, commaPos);
            if (fullName.length() > commaPos) {
                firstName = fullName.substring(commaPos + 1);
            }
        } else if (spacePos > 0) {
            firstName = fullName.substring(0, spacePos);
            if (fullName.length() > spacePos) {
                lastName = fullName.substring(spacePos);
            }
        } else if (StringUtils.isEmpty(fullName)) {
            firstName = "";
            lastName = "";
        } else {
            firstName = fullName;
            lastName = "";
        }
        Name result = new Name();
        if (firstName != null) {
            result.setFirst(firstName.trim());
        }
        if (lastName != null) {
            result.setLast(lastName.trim());
        }
        return result;

    }
}
