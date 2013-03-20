package com.scandilabs.framework.entity.support;

/**
 * Marker interface for objects that can be uniquely identified by a single id.
 * Ensures consistent identification of entities across an application.
 * 
 * @author mkvalsvik
 * 
 */
public interface Identifiable extends Comparable {

    public Long getId();

}
