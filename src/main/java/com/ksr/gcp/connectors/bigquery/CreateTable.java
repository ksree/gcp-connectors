package com.ksr.gcp.connectors.bigquery;

import com.google.cloud.bigquery.*;
import com.ksr.Action;
import com.typesafe.config.Config;

public class CreateTable implements Action {

    public static void runCreateTable() {
        // TODO(developer): Replace these variables before running the sample.
        String datasetName = "MY_DATASET_NAME";
        String tableName = "MY_TABLE_NAME";
        Schema schema =
                Schema.of(
                        Field.of("stringField", StandardSQLTypeName.STRING),
                        Field.of("booleanField", StandardSQLTypeName.BOOL));
        createTable(datasetName, tableName, schema);
    }

    public static void createTable(String datasetName, String tableName, Schema schema) {
        try {
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            TableId tableId = TableId.of(datasetName, tableName);
            TableDefinition tableDefinition = StandardTableDefinition.of(schema);
            TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

            bigquery.create(tableInfo);
            System.out.println("Table created successfully");
        } catch (BigQueryException e) {
            System.out.println("Table was not created. \n" + e.toString());
        }
    }

    @Override
    public void execute(Config conf) {

    }
}
