package com.github.gquintana.elasticsearch.data;

public class ConstantDataProvider extends DataProvider {
    private final String index;
    private final String type;

    public ConstantDataProvider(String index, String type) {
        this.index = index;
        this.type = type;
    }

    @Override
    protected Data doProvide(int rowIndex) {
        return new Data(index, type, null);
    }
}
