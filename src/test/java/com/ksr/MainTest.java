package com.ksr;

import org.junit.Test;

public class MainTest {
    @Test
    public void testGetBucketMetadata() {
        Main.main(new String[]{"GetBucketMetadata"});
    }

    @Test
    public void testCreateDataset() {
        Main.main(new String[]{"CreateBQDataset"});
    }

    @Test
    public void testLoadAvroFromGCS() {
        Main.main(new String[]{"LoadAvroFromGCS"});
    }
}
