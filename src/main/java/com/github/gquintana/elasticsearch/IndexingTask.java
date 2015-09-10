package com.github.gquintana.elasticsearch;

/**
 * Base class for indexing tasks
 */
public abstract class IndexingTask extends Task {
    protected int bulkSize;

    public IndexingTask(DataProvider dataProvider, TemplatingService templatingService) {
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
