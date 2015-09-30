package com.github.gquintana.elasticsearch.search;

import com.github.gquintana.elasticsearch.Task;
import com.github.gquintana.elasticsearch.data.DataProvider;
import com.github.gquintana.elasticsearch.data.TemplatingService;

public abstract class SearchTask extends Task {
    public SearchTask(DataProvider dataProvider, TemplatingService templatingService) {
        super(dataProvider, templatingService);
    }

    public String toString() {
        return "search";
    }

}
