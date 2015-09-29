package com.github.gquintana.elasticsearch;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

/**
 * JSON utilities
 */
public class Jsons {
    /**
     * Serialize Map of Map into a String containing JSON
     * @param map
     * @return
     * @throws IOException
     */
    public static String convertToString(Map<String, Object> map) throws IOException {
        return JsonXContent.contentBuilder().map(map).string();
    }

    /**
     * Deserialize JSON Stream into Map of Map
     */
    public static Map<String, Object> parseMap(InputStream inputStream) throws IOException {
        return parser(inputStream).map();
    }
    private static XContentParser parser(InputStream inputStream) throws IOException {
        return JsonXContent.jsonXContent.createParser(inputStream);
    }
}
