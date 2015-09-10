package com.github.gquintana.elasticsearch.index;

import com.beust.jcommander.Parameter;
import com.github.gquintana.elasticsearch.Command;
import com.github.gquintana.elasticsearch.Task;
import com.github.gquintana.elasticsearch.data.DataProvider;
import com.github.gquintana.elasticsearch.data.TemplatingService;

public class IndexCommand extends Command {
    @Parameter(names = {"-b", "--bulk-size"}, description = "Bulk size")
    private Integer bulkSize;

    @Override
    protected Task createTask() {
        DataProvider dataProvider = createDataProvider();
        TemplatingService templatingService = new TemplatingService();
        IndexTask task = createTaskFactory().indexingTask(dataProvider, templatingService);
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
