package org.catamarancode.entity.support;

/**
 * Thrown when a persistent entity cannot be found. Exists in order to not
 * expose Hibernate exceptions directly (reduce tight coupling to Hibernate)
 * 
 * @author mkvalsvik
 * 
 */
public class RepositoryObjectNotFoundException extends RuntimeException {

    private static final long serialVersionUID = 149252521L;

    public RepositoryObjectNotFoundException(String message) {
        super(message);
    }

    public RepositoryObjectNotFoundException(String message, Exception e) {
        super(message, e);
    }
}
