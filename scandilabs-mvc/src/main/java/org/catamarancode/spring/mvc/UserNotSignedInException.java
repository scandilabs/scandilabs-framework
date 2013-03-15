package org.catamarancode.spring.mvc;

public class UserNotSignedInException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public UserNotSignedInException() {
    }

    public UserNotSignedInException(String message) {
        super(message);
    }

    public UserNotSignedInException(Throwable cause) {
        super(cause);
    }

    public UserNotSignedInException(String message, Throwable cause) {
        super(message, cause);
    }

}
