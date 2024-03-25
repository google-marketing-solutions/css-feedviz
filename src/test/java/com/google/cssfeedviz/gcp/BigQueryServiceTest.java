// Copyright 2024 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.cssfeedviz.gcp;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cssfeedviz.utils.AccountInfo;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class BigQueryServiceTest {

  private final String TEST_CONFIG_DIR = "./config/test";
  private final String ACCOUNT_INFO_FILE_NAME = "account-info.json";
  private final String TEST_DATASET_NAME = "TEST_DATASET";
  private final String TEST_LOCATION = "EU";
  private final DatasetId DATASET_ID = DatasetId.of(TEST_DATASET_NAME);
  private final DatasetInfo DATASET_INFO =
      DatasetInfo.newBuilder(TEST_DATASET_NAME).setLocation(TEST_LOCATION).build();

  private AccountInfo accountInfo;
  private BigQueryService bigQueryService;

  @Mock private BigQuery bigQuery;
  @Mock private Dataset dataset;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    accountInfo = AccountInfo.load(TEST_CONFIG_DIR, ACCOUNT_INFO_FILE_NAME);
    bigQueryService = new BigQueryService(accountInfo);
    bigQueryService.setBigQuery(bigQuery);
  }

  @Test
  public void datasetExists_datasetExists() throws IOException {
    when(bigQuery.getDataset(DATASET_ID)).thenReturn(dataset);
    assertTrue(bigQueryService.datasetExists(TEST_DATASET_NAME));
  }

  @Test
  public void datasetExists_datasetDoesNotExist() throws IOException {
    when(bigQuery.getDataset(DATASET_ID)).thenReturn(null);
    assertFalse(bigQueryService.datasetExists(TEST_DATASET_NAME));
  }

  @Test
  public void createDataset() throws IOException {
    when(bigQuery.create(DATASET_INFO)).thenReturn(dataset);
    assertEquals(dataset, bigQueryService.createDataset(TEST_DATASET_NAME, TEST_LOCATION));
  }
}
