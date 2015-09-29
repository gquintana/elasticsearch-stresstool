package com.github.gquintana.elasticsearch;

/**
 * Wraps a Jest Exception
 */
public class JestException extends EsStressToolException {
    public JestException(String message) {
        super(message);
    }

    public JestException(String message, Throwable cause) {
        super(message, cause);
    }

    public JestException(Throwable cause) {
        super(cause);
    }
}
