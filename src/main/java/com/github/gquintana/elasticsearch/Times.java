package com.github.gquintana.elasticsearch;

import java.util.concurrent.TimeUnit;

public class Times {
    public static String formatForHuman(long duration, TimeUnit timeUnit) {
        long millis = timeUnit.toMillis(duration);
        StringBuilder stringBuilder = new StringBuilder();
        if (millis > 1000) {
            long secs = millis / 1000;
            millis = millis % 1000;
            if (secs > 60) {
                long mins = secs / 60;
                secs = secs % 60;
                if (mins > 60) {
                    long hours = mins / 60;
                    mins = mins % 60;
                    stringBuilder.append(hours).append('h');
                }
                stringBuilder.append(mins).append('m');
            }
            stringBuilder.append(secs).append('s');
        }
        stringBuilder.append(millis).append("ms");
        return stringBuilder.toString();
    }
}
