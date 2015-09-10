package com.github.gquintana.elasticsearch.search;

import com.github.gquintana.elasticsearch.Command;
import com.github.gquintana.elasticsearch.Task;
import com.github.gquintana.elasticsearch.data.DataProvider;
import com.github.gquintana.elasticsearch.data.TemplatingService;

public class SearchCommand extends Command {

    @Override
    protected Task createTask() {
        DataProvider dataProvider = createDataProvider();
        TemplatingService templatingService = new TemplatingService();
        Task task = createTaskFactory().searchingTask(dataProvider, templatingService);
        if (docTemplate != null) task.setTemplateLocation(docTemplate);
        return task;
    }
}
