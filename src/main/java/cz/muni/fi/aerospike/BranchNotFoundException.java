package cz.muni.fi.aerospike;

/**
 * Created by mihu on 12.5.16.
 */
public class BranchNotFoundException extends Exception {

    public BranchNotFoundException() {
    }

    public BranchNotFoundException(String message) {
        super(message);
    }

    public BranchNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public BranchNotFoundException(Throwable cause) {
        super(cause);
    }

    public BranchNotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
