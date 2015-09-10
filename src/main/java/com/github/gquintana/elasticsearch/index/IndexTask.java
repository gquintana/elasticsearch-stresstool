package com.github.gquintana.elasticsearch.index;

import com.github.gquintana.elasticsearch.Task;
import com.github.gquintana.elasticsearch.data.DataProvider;
import com.github.gquintana.elasticsearch.data.TemplatingService;

/**
 * Base class for indexing tasks
 */
public abstract class IndexTask extends Task {
    protected int bulkSize;

    public IndexTask(DataProvider dataProvider, TemplatingService templatingService) {
        super(dataProvider, templatingService);
    }

    public int getBulkSize() {
        return bulkSize;
    }

    public void setBulkSize(int bulkSize) {
        this.bulkSize = bulkSize;
    }

    protected boolean shouldBulkIndex() {
        return bulkSize >= 2;
    }

}
