package com.github.gquintana.elasticsearch.data;

import java.util.HashMap;
import java.util.Map;

public class Data {
    private final String index;
    private final String type;
    private final String id;
    private final Map<String, Object> source;

    public Data(String index, String type, String id, Map<String, Object> source) {
        this.index = index;
        this.type = type;
        this.id = id;
        this.source = source;
    }

    public Data(String index, String type, String id) {
        this(index, type, id, new HashMap<>());
    }

    public String getIndex() {
        return index;
    }
    public String[] getIndices() {
        return index == null ? new String[0] : index.split(",");
    }
    public String getType() {
        return type;
    }
    public String[] getTypes() {
        return type == null ? new String[0] : type.split(",");
    }

    public String getId() {
        return id;
    }

    public void setSource(Map<String, Object> source) {
        this.source.putAll(source);
    }
    public void clearSource() {
        this.source.clear();
    }
    public Map<String, Object> getSource() {
        return source;
    }
    public Object getSourceAt(String name) {
        return source.get(name);
    }
    public void setSourceAt(String name, Object value) {
        source.put(name, value);
    }
}
