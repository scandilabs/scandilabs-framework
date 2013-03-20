package com.scandilabs.framework.entity.support;

/**
 * Provides convenient default implementations of common java functions for
 * Identifiable entities. Ensures correct behavior in Collections and object
 * comparisons.
 * 
 * @author mkvalsvik
 * 
 */
public abstract class IdentifiableBase implements Identifiable {

    public int hashCode() {
        return 0 != getId() ? Long.valueOf(getId()).hashCode() : "NULL"
                .hashCode();
    }

    public boolean equals(Object obj) {

        if (null == obj)
            return false;

        // Note: Removed this because it fails to identify two identical objects
        // if one of them is enhanced by cglib
        /*
         * if( !obj.getClass().getName().equals( getClass().getName())) return
         * false;
         */

        // Right type of object?
        if (!(obj instanceof IdentifiableBase)) {
            return false;
        }

        return 0 != getId() ? getId() == ((IdentifiableBase) obj).getId()
                : 0 == ((IdentifiableBase) obj).getId();
    }

    public int compareTo(Object obj) {

        if (null == obj)
            return 1;

        if (obj.getClass().getName().equals(getClass().getName())) {
            Long myId = Long.valueOf(this.getId());
            Long otherId = Long.valueOf(((IdentifiableBase) obj).getId());
            if (null != myId) {
                return myId.compareTo(otherId);
            } else {
                return -1;
            }
        }

        return -1;
    }

    public String toString() {
        return "id=" + this.getId() + ": " + super.toString();
    }
}