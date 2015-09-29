package com.github.gquintana.elasticsearch;

import java.io.*;
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
    public static byte[] loadBytes(String resource) throws IOException {
        try(InputStream inputStream=open(resource)) {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            int bufferLen;
            while((bufferLen = inputStream.read(buffer)) >= 0 ) {
                outputStream.write(buffer, 0, bufferLen);
            }
            return outputStream.toByteArray();
        }
    }
}
