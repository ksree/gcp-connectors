package com.ksr.gcp.connectors.bigquery;

import com.google.cloud.bigquery.*;
import com.ksr.Action;
import com.typesafe.config.Config;

public class LoadAvroFromGCS implements Action {

    public static void runLoadAvroFromGCS(Config config) {

        String datasetName = config.getString("bigquery.datasetName");
        String tableName = config.getString("bigquery.tableName");
        String sourceUri = config.getString("bigquery.sourceUri");
        loadAvroFromGCS(datasetName, tableName, sourceUri);
    }

    public static void loadAvroFromGCS(String datasetName, String tableName, String sourceUri) {
        try {
            // Initialize client that will be used to send requests. This client only needs to be created
            // once, and can be reused for multiple requests.
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();

            TableId tableId = TableId.of(datasetName, tableName);
            LoadJobConfiguration loadConfig =
                    LoadJobConfiguration.of(tableId, sourceUri, FormatOptions.avro());

            // Load data from a GCS Avro file into the table
            Job job = bigquery.create(JobInfo.of(loadConfig));
            // Blocks until this load table job completes its execution, either failing or succeeding.
            job = job.waitFor();
            if (job.isDone()) {
                System.out.println("Avro from GCS successfully loaded in a table");
            } else {
                System.out.println(
                        "BigQuery was unable to load into the table due to an error:"
                                + job.getStatus().getError());
            }
        } catch (BigQueryException | InterruptedException e) {
            System.out.println("Column not added during load append \n" + e.toString());
        }
    }

    @Override
    public void execute(Config conf) {
        LoadAvroFromGCS.runLoadAvroFromGCS(conf);
    }
}
