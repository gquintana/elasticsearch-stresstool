package com.github.gquintana.elasticsearch.search;


import com.github.gquintana.elasticsearch.JestException;
import com.github.gquintana.elasticsearch.Jests;
import com.github.gquintana.elasticsearch.Jsons;
import com.github.gquintana.elasticsearch.Task;
import com.github.gquintana.elasticsearch.data.Data;
import com.github.gquintana.elasticsearch.data.DataProvider;
import com.github.gquintana.elasticsearch.data.TemplatingService;
import io.searchbox.client.JestClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;

import java.io.IOException;
import java.util.Arrays;

/**
 * Service to index using transport protocol
 */
public class JestSearchTask extends Task {
    private final JestClient client;

    public JestSearchTask(JestClient client, DataProvider dataProvider, TemplatingService templatingService) {
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
            SearchResult searchResult = Jests.execute(client, search, "Search");
        } catch (IOException e) {
            throw new JestException(e);
        }
    }

    public JestClient getClient() {
        return client;
    }
}
