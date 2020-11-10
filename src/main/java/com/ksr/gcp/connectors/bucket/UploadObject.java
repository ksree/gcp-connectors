package com.ksr.gcp.connectors.bucket;

import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import com.ksr.Action;
import com.typesafe.config.Config;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class UploadObject implements Action {

    // The ID of your GCP project
    // String projectId = "your-project-id";

    // The ID of your GCS bucket
    // String bucketName = "your-unique-bucket-name";

    // The ID of your GCS object
    // String objectName = "your-object-name";

    // The path to your file to upload
    // String filePath = "path/to/your/file"
    public static void uploadObject(
            String projectId, String bucketName, String objectName, String filePath) throws IOException {


        Storage storage = StorageOptions.newBuilder().setProjectId(projectId).build().getService();
        BlobId blobId = BlobId.of(bucketName, objectName);
        BlobInfo blobInfo = BlobInfo.newBuilder(blobId).build();
        storage.create(blobInfo, Files.readAllBytes(Paths.get(filePath)));

        System.out.println(
                "File " + filePath + " uploaded to bucket " + bucketName + " as " + objectName);
    }

    public static void main(String[] args) throws IOException {
        String projectId = "kapilsreed12-1dataflow";
        String bucketName = "kapilsreed12-dataflow-test998990";
        //String objectName = "your-object-name";
        //String filePath = "path/to/your/file";
/*        String schemaFile = "C:\\Users\\Kapil.Sreedharan\\Downloads\\userdata.avsc";
        String dataFile = "C:\\Users\\Kapil.Sreedharan\\Downloads\\userdata1.avro";*/
        String dataFile = "C:\\Users\\Kapil.Sreedharan\\Downloads\\ork_data.avsc";
        uploadObject(projectId, bucketName, "ork_data.avsc", dataFile);

    }
    @Override
    public void execute(Config config) {
        try {
            uploadObject(config.getString("gcs.projectId"), config.getString("gcs.bucketName"), config.getString("gcs.objectName"), config.getString("gcs.dataFile"));
        } catch (IOException e) {
            e.printStackTrace();
        }

    }
}

