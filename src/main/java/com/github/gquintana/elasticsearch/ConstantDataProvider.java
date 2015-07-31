package com.github.gquintana.elasticsearch;

/**
 * Created by gquintana on 30/07/15.
 */
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
