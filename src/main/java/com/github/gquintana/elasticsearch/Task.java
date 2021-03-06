package com.github.gquintana.elasticsearch;

import com.github.gquintana.elasticsearch.data.Data;
import com.github.gquintana.elasticsearch.data.DataProvider;
import com.github.gquintana.elasticsearch.data.TemplatingService;

public abstract class Task {
    private final DataProvider dataProvider;
    private final TemplatingService templatingService;
    private String templateLocation;

    public Task(DataProvider dataProvider, TemplatingService templatingService) {
        this.dataProvider = dataProvider;
        this.templatingService = templatingService;
    }
    protected Data provideData() {
        Data data;
        if (templateLocation == null) {
            data = dataProvider.provide();
        } else {
            data = dataProvider.provide(templatingService, templateLocation);
        }
        return data;
    }

    /**
     * Called before all iterations
     */
    public void prepare() {

    }

    /**
     * Called for each iteration
     */
    public abstract void execute();
    public DataProvider getDataProvider() {
        return dataProvider;
    }

    public TemplatingService getTemplatingService() {
        return templatingService;
    }

    public String getTemplateLocation() {
        return templateLocation;
    }

    public void setTemplateLocation(String templateLocation) {
        this.templateLocation = templateLocation;
    }
}
