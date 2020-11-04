package com.ksr.gcp.connectors.bucket;

import com.google.api.gax.paging.Page;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.*;
import com.google.common.collect.Lists;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Map;

public class GetBucketMetadata {
    public static void getBucketMetadata(String projectId, String bucketName) {
        // The ID of your GCP project
        // String projectId = "your-project-id";

        // The ID of your GCS bucket
        // String bucketName = "your-unique-bucket-name";

        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();

        // Select all fields. Fields can be selected individually e.g. Storage.BucketField.NAME
        Bucket bucket =
                storage.get(bucketName, Storage.BucketGetOption.fields(Storage.BucketField.values()));

        // Print bucket metadata
        System.out.println("BucketName: " + bucket.getName());
        System.out.println("DefaultEventBasedHold: " + bucket.getDefaultEventBasedHold());
        System.out.println("DefaultKmsKeyName: " + bucket.getDefaultKmsKeyName());
        System.out.println("Id: " + bucket.getGeneratedId());
        System.out.println("IndexPage: " + bucket.getIndexPage());
        System.out.println("Location: " + bucket.getLocation());
        System.out.println("LocationType: " + bucket.getLocationType());
        System.out.println("Metageneration: " + bucket.getMetageneration());
        System.out.println("NotFoundPage: " + bucket.getNotFoundPage());
        System.out.println("RetentionEffectiveTime: " + bucket.getRetentionEffectiveTime());
        System.out.println("RetentionPeriod: " + bucket.getRetentionPeriod());
        System.out.println("RetentionPolicyIsLocked: " + bucket.retentionPolicyIsLocked());
        System.out.println("RequesterPays: " + bucket.requesterPays());
        System.out.println("SelfLink: " + bucket.getSelfLink());
        System.out.println("StorageClass: " + bucket.getStorageClass().name());
        System.out.println("TimeCreated: " + bucket.getCreateTime());
        System.out.println("VersioningEnabled: " + bucket.versioningEnabled());
        if (bucket.getLabels() != null) {
            System.out.println("\n\n\nLabels:");
            for (Map.Entry<String, String> label : bucket.getLabels().entrySet()) {
                System.out.println(label.getKey() + "=" + label.getValue());
            }
        }
        if (bucket.getLifecycleRules() != null) {
            System.out.println("\n\n\nLifecycle Rules:");
            for (BucketInfo.LifecycleRule rule : bucket.getLifecycleRules()) {
                System.out.println(rule);
            }
        }
    }

    static void authExplicit(String jsonPath) throws IOException {
        // You can specify a credential file by providing a path to GoogleCredentials.
        // Otherwise credentials are read from the GOOGLE_APPLICATION_CREDENTIALS environment variable.
        GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream(jsonPath))
                .createScoped(Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
        Storage storage = StorageOptions.newBuilder().setCredentials(credentials).build().getService();

        System.out.println("Buckets:");
        Page<Bucket> buckets = storage.list();
        for (Bucket bucket : buckets.iterateAll()) {
            System.out.println(bucket.toString());
        }
    }

    static void readBucket() throws IOException {
        String PROJECT_ID =  "kapilsreed12-1dataflow";
        String PATH_TO_JSON_KEY = "/path/to/json/key";
        String BUCKET_NAME = "charlotte-kapil-wedding-photos";
        String OBJECT_NAME = "my-object";

        StorageOptions options = StorageOptions.newBuilder()
                .setProjectId(PROJECT_ID)
                .setCredentials(GoogleCredentials.fromStream(
                        new FileInputStream(PATH_TO_JSON_KEY))).build();

        Storage storage = options.getService();
        Blob blob = storage.get(BUCKET_NAME, OBJECT_NAME);
        ReadChannel r = blob.reader();
    }

    public static void main(String args[]) throws IOException {
        String projectId = "kapilsreed12-1dataflow";
        String bucketName = "charlotte-kapil-wedding-photos";
       // GetBucketMetadata.authExplicit("C:\\projects\\bdaa-dl2x-dev-sc-be0su7ip-bb6f7a8d6637.json");
        GetBucketMetadata.getBucketMetadata(projectId, bucketName);

    }

}