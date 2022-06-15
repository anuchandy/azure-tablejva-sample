package com.aztable.sample;

import com.azure.core.http.rest.PagedIterable;
import com.azure.data.tables.TableClient;
import com.azure.data.tables.TableServiceClient;
import com.azure.data.tables.TableServiceClientBuilder;
import com.azure.data.tables.models.ListEntitiesOptions;
import com.azure.data.tables.models.TableEntity;

import java.util.List;
import java.util.stream.Collectors;

public class App {
    // A pre-created table.
    private static final String tableName = "tbl1";
    private final TableClient tableClient;

    public static void main( String[] args ) {
        final App app = new App();

        // Creating 200K entities
        // app.populateTable("p1", 100000);
        // app.populateTable("p2", 25000);
        // app.populateTable("p3", 25000);
        // app.populateTable("p4", 25000);
        // app.populateTable("p5", 25000);

        // Keep fetching forever
        app.fetchAndConsumeForEver(new ListEntitiesOptions());
    }

    public App() {
        final String connString = System.getenv("CON_STR");
        final TableServiceClient tableServiceClient = new TableServiceClientBuilder()
                .connectionString(connString)
                .buildClient();

        tableClient = tableServiceClient.getTableClient(tableName);
    }

    public void fetchAndConsumeForEver(ListEntitiesOptions options) {
        // Fetch from table forever...
        long iterationId = 0;
        while (true) {
            fetchAndConsume(options, iterationId);
            iterationId = (iterationId + 1 == Long.MAX_VALUE - 1) ? 0 : iterationId + 1;
        }
    }

    public void fetchAndConsume(ListEntitiesOptions options, long iterationId) {
        PagedIterable<TableEntity> tableEntities = tableClient.listEntities(options, null, null);

        List<String> rowIds = tableEntities.streamByPage()
                .flatMap(response -> {
                    return response.getElements()
                            .stream()
                            .map(e -> e.getRowKey());
                })
                .collect(Collectors.toList());

        int n = 0;
        for (String ignored : rowIds) {
            n++;
        }
        System.out.println("IterationId:" + iterationId + " Entity count:" + n);
    }

    public void populateTable(String partitionKey, int rowCount) {
        for (int row = 0; row < rowCount; row++) {
            String rowKey = String.valueOf(row);
            TableEntity entity = new TableEntity(partitionKey, rowKey)
                    .addProperty("Product", "Marker Set")
                    .addProperty("Price", 5.00)
                    .addProperty("Quantity", 21);

            tableClient.createEntity(entity);
        }
    }
}
