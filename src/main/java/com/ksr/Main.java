package com.ksr;

import com.ksr.gcp.connectors.bigquery.CreateBQDataset;
import com.ksr.gcp.connectors.bigquery.LoadAvroFromGCS;
import com.ksr.gcp.connectors.bucket.CreateStorageBucket;
import com.ksr.gcp.connectors.bucket.GetBucketMetadata;
import com.ksr.gcp.connectors.bucket.UploadObject;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.util.HashMap;
import java.util.Map;

import static java.lang.System.exit;


public class Main {

    public static void main(String[] args) {
        Config conf = ConfigFactory.load();
        Map<String, Action> actions = new HashMap<String, Action>();
        actions.put("CreateGCSBucket", new CreateStorageBucket());
        actions.put("GetBucketMetadata", new GetBucketMetadata());
        actions.put("UploadObject", new UploadObject());
        actions.put("CreateBQDataset", new CreateBQDataset());
        actions.put("LoadAvroFromGCS", new LoadAvroFromGCS());

        String inputAction = "";
        if (args.length > 0) {
            inputAction = args[0];
        } else {
            System.out.println("No Action provided");
            exit(0);
        }

        if (!actions.containsKey(inputAction)) {
            System.out.println("Action not found" + inputAction);
            exit(0);
        }

        System.out.println("Executing " + inputAction);
        actions.get(inputAction).execute(conf);
    }

}
