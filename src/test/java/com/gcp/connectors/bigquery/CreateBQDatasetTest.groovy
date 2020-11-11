package com.gcp.connectors.bigquery

import com.ksr.gcp.connectors.bigquery.CreateBQDataset
import org.junit.Test

class CreateBQDatasetTest {
    @Test
    void testCreateDataset() {
        assert(CreateBQDataset.createDataset("testdataset999"))
    }
}
