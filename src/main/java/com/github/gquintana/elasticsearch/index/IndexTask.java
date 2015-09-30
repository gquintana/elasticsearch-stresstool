package com.github.gquintana.elasticsearch.index;

import com.github.gquintana.elasticsearch.Task;
import com.github.gquintana.elasticsearch.data.DataProvider;
import com.github.gquintana.elasticsearch.data.TemplatingService;

/**
 * Base class for indexing tasks
 */
public abstract class IndexTask extends Task {
    protected int bulkSize;
    protected boolean indexDelete = false;
    protected String indexName;
    protected String indexSettings;

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

    public boolean isIndexDelete() {
        return indexDelete;
    }

    public void setIndexDelete(boolean indexDelete) {
        this.indexDelete = indexDelete;
    }

    public String getIndexName() {
        return indexName;
    }

    public void setIndexName(String indexName) {
        this.indexName = indexName;
    }

    public String getIndexSettings() {
        return indexSettings;
    }

    public void setIndexSettings(String indexSettings) {
        this.indexSettings = indexSettings;
    }

    public String toString() {
        return shouldBulkIndex() ? bulkSize +" bulk index" : "index";
    }
}
