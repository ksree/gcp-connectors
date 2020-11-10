package com.gcp.connectors.bigquery

import com.ksr.gcp.connectors.bigquery.CreateDataset
import org.junit.Test

class CreateDatasetTest  {
    @Test
    void testCreateDataset() {
        assert(CreateDataset.createDataset("testdataset999"))
    }
}
