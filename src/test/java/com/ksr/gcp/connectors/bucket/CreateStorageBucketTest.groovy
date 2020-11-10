package com.ksr.gcp.connectors.bucket

import com.google.cloud.storage.Bucket
import org.junit.Test


class CreateStorageBucketTest {

    @Test
    void testCreateBucket() {
        Bucket bucket = CreateStorageBucket.createBucket("gcp-connectors-testbucket999")
        Assert.assertTrue (bucket.exists());
    }
}
