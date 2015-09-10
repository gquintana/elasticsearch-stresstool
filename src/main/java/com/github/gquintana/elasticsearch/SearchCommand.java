package com.github.gquintana.elasticsearch;

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
