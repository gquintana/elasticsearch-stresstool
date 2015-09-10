package com.github.gquintana.elasticsearch;

import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Map;

/**
 * JSON utilities
 */
public class Jsons {
    public static String convertToString(Map<String, Object> map) throws IOException {
        return JsonXContent.contentBuilder().map(map).string();
    }
}
