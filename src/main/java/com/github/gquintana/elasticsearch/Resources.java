package com.github.gquintana.elasticsearch;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Files and Resources uilities
 */
public class Resources {
    public static String classResource(Class<?> clazz, String resource) {
        return "classpath:/"+clazz.getPackage().getName().replaceAll("\\.","/")+"/"+resource;
    }
    public static InputStream open(String resource) throws IOException {
        Matcher locationMatcher = Pattern.compile("^([a-z]+):/*(.*)$").matcher(resource);
        InputStream inputStream;
        if (locationMatcher.matches()) {
            String scheme = locationMatcher.group(1);
            if (scheme.equals("classpath")) {
                String resourcePath = locationMatcher.group(2);
                inputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(resourcePath);
                if (inputStream == null) {
                    throw new FileNotFoundException("Resource " + resourcePath + " not found");
                }
            } else {
                inputStream = new URL(resource).openStream();
            }
        } else {
            inputStream = new FileInputStream(resource);
        }
        return inputStream;
    }
}
