package com.github.gquintana.elasticsearch;

import io.searchbox.client.JestResult;

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
    public static void handleResult(JestResult result, String message) {
        if (!result.isSucceeded()) {
            throw new JestException(message+": "+result.getErrorMessage());
        }
    }
    public static void handleException(RuntimeException e) {
        // There is no Jest Root exception :-(
        if (e.getClass().getName().startsWith("io.searchbox.")) {
            throw new JestException(e);
        } else {
            throw e;
        }
    }
}
