package com.ksr.gcp.connectors.bucket;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.ksr.Action;
import com.typesafe.config.Config;

import java.io.IOException;

public class CreateStorageBucket implements Action {
    public static Bucket createBucket(String bucketName){
        // Instantiates a client
        Storage storage = StorageOptions.getDefaultInstance().getService();
        // Creates the new bucket
        Bucket bucket = storage.create(BucketInfo.of(bucketName));
        System.out.printf("Bucket %s created.%n", bucket.getName());
        return bucket;
    }

    public static void main(String args[]) throws IOException {
        createBucket("kapilsreed12-dataflow-test998990");
    }

    @Override
    public void execute(Config config ) {
        createBucket(config.getString("gcs.bucketName"));
    }
}
