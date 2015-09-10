package com.github.gquintana.elasticsearch.data;

import com.github.gquintana.elasticsearch.EsStressToolException;
import com.github.gquintana.elasticsearch.Resources;
import com.opencsv.CSVReader;

import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

public class CsvDataProvider extends DataProvider {
    private final String csvLocation;
    private CSVReader csvReader;
    private String[] csvHeader;
    private Charset charset = Charset.defaultCharset();
    private String indexColumn = "docIndex";
    private String typeColumn = "docType";
    private String idColumn = "docId";
    private String indexDefault = "index";
    private String typeDefault = "type";

    public CsvDataProvider(String csvLocation) {
        this.csvLocation = csvLocation;
    }

    private String[] open() throws IOException {
        csvReader = new CSVReader(new InputStreamReader(Resources.open(csvLocation), charset));
        if (csvLocation.equals("classpath:/config/names.txt")) {
            csvHeader = new String[]{"name"};
        } else {
            csvHeader = csvReader.readNext();
            if (csvHeader == null) {
                throw new EsStressToolException("Empty CSV in " + csvLocation);

            }
        }
        String[] csvRow = csvReader.readNext();
        if (csvRow == null) {
            throw new EsStressToolException("No data in " + csvLocation);
        }
        return csvRow;
    }

    private synchronized String[] provideRow() {
        try {
            String[] csvRow;
            if (csvReader == null) {
                csvRow = open();
            } else {
                csvRow = csvReader.readNext();
                if (csvRow == null) {
                    csvReader.close();
                    csvRow = open();
                }
            }
            return csvRow;
        } catch (IOException e) {
            throw new EsStressToolException(e);
        }
    }

    private static String get(Map<String, Object> map, String key, String defaultValue) {
        if (key == null) return defaultValue;
        String value = (String) map.remove(key);
        if (value == null) return defaultValue;
        return value;
    }

    @Override
    protected Data doProvide(int rowIndex) {
        String[] csvRow = provideRow();
        Map<String, Object> map = new HashMap<>();
        for (int i = 0; i < Math.min(csvHeader.length, csvRow.length); i++) {
            map.put(csvHeader[i], csvRow[i]);
        }
        String index = get(map, indexColumn, indexDefault);
        String type = get(map, typeColumn, typeDefault);
        String id = get(map, idColumn, null);
        return new Data(index, type, id, map);
    }

    public Charset getCharset() {
        return charset;
    }

    public void setCharset(Charset charset) {
        this.charset = charset;
    }

    public String getIndexColumn() {
        return indexColumn;
    }

    public void setIndexColumn(String indexColumn) {
        this.indexColumn = indexColumn;
    }

    public String getTypeColumn() {
        return typeColumn;
    }

    public void setTypeColumn(String typeColumn) {
        this.typeColumn = typeColumn;
    }

    public String getIndexDefault() {
        return indexDefault;
    }

    public void setIndexDefault(String indexDefault) {
        this.indexDefault = indexDefault;
    }

    public String getTypeDefault() {
        return typeDefault;
    }

    public void setTypeDefault(String typeDefault) {
        this.typeDefault = typeDefault;
    }

    public String getIdColumn() {
        return idColumn;
    }

    public void setIdColumn(String idColumn) {
        this.idColumn = idColumn;
    }
}

