package org.catamarancode.entity.support;

/**
 * Thrown when a single unique result was expected in a database query, but
 * multiple matches were found
 * 
 * @author mkvalsvik
 * 
 */
public class NotUniqueResultException extends RuntimeException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public NotUniqueResultException() {
		super();
	}

	public NotUniqueResultException(String arg0) {
		super(arg0);
	}

}
