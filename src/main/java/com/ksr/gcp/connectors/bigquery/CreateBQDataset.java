package com.ksr.gcp.connectors.bigquery;

import com.ksr.Action;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryException;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetInfo;
import com.typesafe.config.Config;

public class CreateBQDataset implements Action {

    public static void createDataset(String datasetName) {
        try {
            // Initialize client that will be used to send requests. This client only needs to be created
            // once, and can be reused for multiple requests.
            BigQuery bigquery = BigQueryOptions.getDefaultInstance().getService();
            DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).build();
            Dataset newDataset = bigquery.create(datasetInfo);
            String dataset = newDataset.getDatasetId().getDataset();
            System.out.println(dataset + " created successfully");
        } catch (BigQueryException e) {
            System.out.println("Dataset was not created. \n" + e.toString());
        }
    }

    @Override
    public void execute(Config config) {
        CreateBQDataset.createDataset(config.getString("bigquery.datasetName"));
    }
}
