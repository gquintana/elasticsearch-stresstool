package com.github.gquintana.elasticsearch.data;

import com.github.gquintana.elasticsearch.EsStressToolException;
import org.elasticsearch.common.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Data generator
 */
public abstract class DataProvider {
    private int nextRowIndex = 0;
    private final RandomData randomData = new RandomData();

    protected abstract Data doProvide(int rowIndex);

    private synchronized int generateRowIndex() {
        return nextRowIndex++;
    }

    public Data provide() {
        return doProvide(generateRowIndex());
    }

    public Data provide(TemplatingService templatingService, String templateLocation) {
        int rowIndex = generateRowIndex();
        Map<String, Object> templateParameters = new HashMap<>();
        templateParameters.put("docNumber", rowIndex);
        templateParameters.put("timestamp", System.currentTimeMillis());
        templateParameters.put("random", randomData);
        Data indexingData = doProvide(rowIndex);
        templateParameters.put("docIndex", indexingData.getIndex());
        templateParameters.put("docType", indexingData.getType());
        templateParameters.put("docId", indexingData.getId());
        templateParameters.putAll(indexingData.getSource());
        try {
            byte[] bytes = templatingService.render(templateLocation, templateParameters);
            Map<String, Object> source = JsonXContent.jsonXContent.createParser(bytes).mapAndClose();
            indexingData.clearSource();
            indexingData.setSource(source);
            return indexingData;
        } catch (IOException e) {
            throw new EsStressToolException("Failed to apply template " + templateLocation, e);
        }
    }
}
