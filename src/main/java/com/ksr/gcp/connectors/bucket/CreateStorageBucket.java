package com.ksr.gcp.connectors.bucket;

import com.google.cloud.storage.Bucket;
import com.google.cloud.storage.BucketInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;

public class CreateStorageBucket {
    public static Bucket createBucket(String bucketName){
        // Instantiates a client
        Storage storage = StorageOptions.getDefaultInstance().getService();
        // Creates the new bucket
        Bucket bucket = storage.create(BucketInfo.of(bucketName));
        System.out.printf("Bucket %s created.%n", bucket.getName());
        return bucket;
    }
}
