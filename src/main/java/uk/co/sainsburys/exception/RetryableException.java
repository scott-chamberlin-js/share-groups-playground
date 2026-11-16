package uk.co.sainsburys.exception;

public class RetryableException extends Exception {
    public RetryableException(String message) {
        super(message);
    }
}
