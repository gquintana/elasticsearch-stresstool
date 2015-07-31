package com.github.gquintana.elasticsearch;

import java.util.Random;

public class RandomData {
    private Random random = new Random();
    public int getInt() {
        return random.nextInt();
    }
    public long getLong() {
        return random.nextLong();
    }
    public boolean getBoolean() {
        return random.nextBoolean();
    }
    public float getFloat() {
        return random.nextFloat();
    }
    public double getDouble() {
        return random.nextDouble();
    }
}
