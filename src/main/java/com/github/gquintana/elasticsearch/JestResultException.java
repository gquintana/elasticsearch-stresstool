package com.github.gquintana.elasticsearch;

import io.searchbox.client.JestResult;

public class JestResultException extends JestException {
    private final JestResult result;
    public JestResultException(String message, JestResult result) {
        super(message);
        this.result = result;
    }

    public JestResult getResult() {
        return result;
    }
}
