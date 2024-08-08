package org.example;

public enum Magics {
    IS_UNEXPECTED_NUMERIC_VAL("-1"),
    IS_EMPTY("EMPTY");

    public final String value;

    Magics(String value) {
        this.value = value;
    }
}
