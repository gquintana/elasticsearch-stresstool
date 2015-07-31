package com.github.gquintana.elasticsearch;

public class EsStressToolException extends RuntimeException {
    public EsStressToolException(String message) {
        super(message);
    }

    public EsStressToolException(String message, Throwable cause) {
        super(message, cause);
    }

    public EsStressToolException(Throwable cause) {
        super(cause);
    }
}
