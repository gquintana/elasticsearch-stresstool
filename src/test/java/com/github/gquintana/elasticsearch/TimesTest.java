package com.github.gquintana.elasticsearch;

import org.junit.Test;

import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class TimesTest {

    @Test
    public void testFormatForHuman() throws Exception {
        assertEquals("1h23m45s678ms", Times.formatForHuman(
                TimeUnit.HOURS.toMillis(1)
                        + TimeUnit.MINUTES.toMillis(23)
                        + TimeUnit.SECONDS.toMillis(45)
                        + 678L
                , TimeUnit.MILLISECONDS));
    }
}