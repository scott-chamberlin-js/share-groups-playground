package uk.co.sainsburys;

public class RetryableException extends Exception {
    public RetryableException(String message) {
        super(message);
    }
}
