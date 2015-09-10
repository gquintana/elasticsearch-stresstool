package com.github.gquintana.elasticsearch;

import com.beust.jcommander.Parameter;

public class IndexCommand extends Command {
    @Parameter(names = {"-b", "--bulk-size"}, description = "Bulk size")
    private Integer bulkSize;

    @Override
    protected Task createTask() {
        DataProvider dataProvider = createDataProvider();
        TemplatingService templatingService = new TemplatingService();
        IndexingTask task = createTaskFactory().indexingTask(dataProvider, templatingService);
        if (bulkSize != null) task.setBulkSize(bulkSize);
        if (docTemplate != null) task.setTemplateLocation(docTemplate);
        return task;
    }

    public Integer getBulkSize() {
        return bulkSize;
    }

    public void setBulkSize(Integer bulkSize) {
        this.bulkSize = bulkSize;
    }
}
