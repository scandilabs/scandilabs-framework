package com.scandilabs.framework.entity.support;

/**
 * Defines an entity that knows how to persist itself
 * 
 * @author mkvalsvik
 * 
 */
public interface Persistable extends Identifiable {

    /**
     * Persist an entity to database
     * 
     * @return the existing unique identifier of the entity, or a new identifier
     *         if persisting for the first time
     */
    public long save();

}
