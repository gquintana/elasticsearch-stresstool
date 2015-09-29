package com.github.gquintana.elasticsearch;

import io.searchbox.action.Action;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;

import java.io.IOException;

/**
 * JEST Utilities to errors and results
 */
public class Jests {
    /**
     * Convert result into {@link JestException} if needed
     */
    public static void handleResult(JestResult result, String message) throws JestResultException {
        if (!result.isSucceeded()) {
            throw new JestResultException(message + ": " + result.getErrorMessage(), result);
        }
    }

    /**
     * Convert exception in {@link JestException}
     *
     * @param e
     */
    public static void handleException(RuntimeException e) throws JestException {
        // There is no Jest Root exception :-(
        if (e.getClass().getName().startsWith("io.searchbox.")) {
            throw new JestException(e);
        } else {
            throw e;
        }
    }

    /**
     * Execute a JEST Action and handle result
     */
    public static <T extends JestResult> T execute(JestClient client, Action<T> action, String actionName) throws JestException {
        try {
            T result = client.execute(action);
            handleResult(result, actionName + " failed");
            return result;
        } catch (IOException e) {
            throw new JestException(e);
        } catch (RuntimeException e) {
            handleException(e);
        }
        return null;
    }
}