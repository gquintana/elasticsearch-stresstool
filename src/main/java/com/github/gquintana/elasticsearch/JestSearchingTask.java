package com.github.gquintana.elasticsearch;


import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;

import java.io.IOException;
import java.util.Arrays;

/**
 * Service to index using transport protocol
 */
public class JestSearchingTask extends Task {
    private final JestClient client;

    public JestSearchingTask(JestClient client, DataProvider dataProvider, TemplatingService templatingService) {
        super(dataProvider, templatingService);
        this.client = client;
    }

    public void execute() {
        Data data = provideData();
        try {
            Search search = new Search.Builder(Jsons.convertToString(data.getSource()))
                    .addIndex(Arrays.asList(data.getIndices()))
                    .addType(Arrays.asList(data.getTypes()))
                    .build();
            SearchResult searchResult = client.execute(search);
            JestException.handleResult(searchResult, "Search failed");
        } catch (IOException e) {
            throw new JestException(e);
        } catch (RuntimeException e) {
            JestException.handleException(e);
        }
    }

    public JestClient getClient() {
        return client;
    }
}
