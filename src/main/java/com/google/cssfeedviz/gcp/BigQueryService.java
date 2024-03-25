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

import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cssfeedviz.utils.AccountInfo;
import com.google.cssfeedviz.utils.Authenticator;
import java.io.IOException;

public class BigQueryService {
  private BigQuery bigQuery;

  public void setBigQuery(BigQuery bigQuery) {
    this.bigQuery = bigQuery;
  }

  public boolean datasetExists(String datasetName) {
    Dataset dataset = this.bigQuery.getDataset(DatasetId.of(datasetName));
    return dataset != null;
  }

  public Dataset createDataset(String datasetName, String location) {
    DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).setLocation(location).build();
    return this.bigQuery.create(datasetInfo);
  }

  public BigQueryService(AccountInfo accountInfo) throws IOException {
    GoogleCredentials googleCredentials = new Authenticator().authenticate(accountInfo);
    BigQueryOptions bigQueryOptions =
        BigQueryOptions.newBuilder().setCredentials(googleCredentials).build();
    this.bigQuery = bigQueryOptions.getService();
  }
}
