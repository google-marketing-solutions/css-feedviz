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
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
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

  public Field getCssProductsAttributesField() {
    Field priceAmountMicros = Field.of("amount_micros", StandardSQLTypeName.INT64);
    Field priceCurrencyCode = Field.of("currency_code", StandardSQLTypeName.STRING);
    Field productDimensionValue = Field.of("value", StandardSQLTypeName.FLOAT64);
    Field productDimensionUnit = Field.of("unit", StandardSQLTypeName.STRING);
    return Field.of(
        "attributes",
        StandardSQLTypeName.STRUCT,
        Field.of("low_price", StandardSQLTypeName.STRUCT, priceAmountMicros, priceCurrencyCode),
        Field.of("high_price", StandardSQLTypeName.STRUCT, priceAmountMicros, priceCurrencyCode),
        Field.of(
            "headline_offer_price",
            StandardSQLTypeName.STRUCT,
            priceAmountMicros,
            priceCurrencyCode),
        Field.of(
            "headline_offer_shipping_price",
            StandardSQLTypeName.STRUCT,
            priceAmountMicros,
            priceCurrencyCode),
        Field.newBuilder("additional_image_links", StandardSQLTypeName.STRING)
            .setMode(Mode.REPEATED)
            .build(),
        Field.newBuilder("product_types", StandardSQLTypeName.STRING)
            .setMode(Mode.REPEATED)
            .build(),
        Field.newBuilder("size_types", StandardSQLTypeName.STRING).setMode(Mode.REPEATED).build(),
        Field.newBuilder(
                "product_details",
                StandardSQLTypeName.STRUCT,
                Field.of("section_name", StandardSQLTypeName.STRING),
                Field.of("attribute_name", StandardSQLTypeName.STRING),
                Field.of("attribute_value", StandardSQLTypeName.STRING))
            .setMode(Mode.REPEATED)
            .build(),
        Field.of(
            "product_weight",
            StandardSQLTypeName.STRUCT,
            productDimensionValue,
            productDimensionUnit),
        Field.of(
            "product_length",
            StandardSQLTypeName.STRUCT,
            productDimensionValue,
            productDimensionUnit),
        Field.of(
            "product_width",
            StandardSQLTypeName.STRUCT,
            productDimensionValue,
            productDimensionUnit),
        Field.of(
            "product_height",
            StandardSQLTypeName.STRUCT,
            productDimensionValue,
            productDimensionUnit),
        Field.newBuilder("product_highlights", StandardSQLTypeName.STRING)
            .setMode(Mode.REPEATED)
            .build(),
        Field.newBuilder(
                "certifications",
                StandardSQLTypeName.STRUCT,
                Field.of("name", StandardSQLTypeName.STRING),
                Field.of("authority", StandardSQLTypeName.STRING),
                Field.of("code", StandardSQLTypeName.STRING))
            .setMode(Mode.REPEATED)
            .build(),
        Field.of("expiration_date", StandardSQLTypeName.TIMESTAMP),
        Field.newBuilder("included_destinations", StandardSQLTypeName.STRING)
            .setMode(Mode.REPEATED)
            .build(),
        Field.newBuilder("excluded_destinations", StandardSQLTypeName.STRING)
            .setMode(Mode.REPEATED)
            .build(),
        Field.of("cpp_link", StandardSQLTypeName.STRING),
        Field.of("cpp_mobile_link", StandardSQLTypeName.STRING),
        Field.of("cpp_ads_redirect", StandardSQLTypeName.STRING),
        Field.of("number_of_offers", StandardSQLTypeName.INT64),
        Field.of("headline_offer_condition", StandardSQLTypeName.STRING),
        Field.of("headline_offer_link", StandardSQLTypeName.STRING),
        Field.of("headline_offer_mobile_link", StandardSQLTypeName.STRING),
        Field.of("title", StandardSQLTypeName.STRING),
        Field.of("image_link", StandardSQLTypeName.STRING),
        Field.of("description", StandardSQLTypeName.STRING),
        Field.of("brand", StandardSQLTypeName.STRING),
        Field.of("mpn", StandardSQLTypeName.STRING),
        Field.of("gtin", StandardSQLTypeName.STRING),
        Field.of("google_product_category", StandardSQLTypeName.STRING),
        Field.of("adult", StandardSQLTypeName.BOOL),
        Field.of("multipack", StandardSQLTypeName.INT64),
        Field.of("is_bundle", StandardSQLTypeName.BOOL),
        Field.of("age_group", StandardSQLTypeName.STRING),
        Field.of("color", StandardSQLTypeName.STRING),
        Field.of("gender", StandardSQLTypeName.STRING),
        Field.of("material", StandardSQLTypeName.STRING),
        Field.of("pattern", StandardSQLTypeName.STRING),
        Field.of("size", StandardSQLTypeName.STRING),
        Field.of("size_system", StandardSQLTypeName.STRING),
        Field.of("item_group_id", StandardSQLTypeName.STRING),
        Field.of("pause", StandardSQLTypeName.STRING),
        Field.of("custom_label_0", StandardSQLTypeName.STRING),
        Field.of("custom_label_1", StandardSQLTypeName.STRING),
        Field.of("custom_label_2", StandardSQLTypeName.STRING),
        Field.of("custom_label_3", StandardSQLTypeName.STRING),
        Field.of("custom_label_4", StandardSQLTypeName.STRING));
  }

  public Field getCssProductsCssProductStatusField() {
    return Field.of(
        "css_product_status",
        StandardSQLTypeName.STRUCT,
        Field.newBuilder(
                "destination_statuses",
                StandardSQLTypeName.STRUCT,
                Field.of("destination", StandardSQLTypeName.STRING),
                Field.newBuilder("approved_countries", StandardSQLTypeName.STRING)
                    .setMode(Mode.REPEATED)
                    .build(),
                Field.newBuilder("pending_countries", StandardSQLTypeName.STRING)
                    .setMode(Mode.REPEATED)
                    .build(),
                Field.newBuilder("disapproved_countries", StandardSQLTypeName.STRING)
                    .setMode(Mode.REPEATED)
                    .build())
            .setMode(Mode.REPEATED)
            .build(),
        Field.newBuilder(
                "item_level_issues",
                StandardSQLTypeName.STRUCT,
                Field.of("code", StandardSQLTypeName.STRING),
                Field.of("servability", StandardSQLTypeName.STRING),
                Field.of("resolution", StandardSQLTypeName.STRING),
                Field.of("attribute", StandardSQLTypeName.STRING),
                Field.of("destination", StandardSQLTypeName.STRING),
                Field.of("description", StandardSQLTypeName.STRING),
                Field.of("detail", StandardSQLTypeName.STRING),
                Field.of("documentation", StandardSQLTypeName.STRING),
                Field.newBuilder("applicable_countries", StandardSQLTypeName.STRING)
                    .setMode(Mode.REPEATED)
                    .build())
            .setMode(Mode.REPEATED)
            .build(),
        Field.of("creation_date", StandardSQLTypeName.TIMESTAMP),
        Field.of("last_update_date", StandardSQLTypeName.TIMESTAMP),
        Field.of("google_expiration_date", StandardSQLTypeName.TIMESTAMP));
  }

  public Schema getCssProductsSchema() {
    return Schema.of(
        Field.of("date", StandardSQLTypeName.DATE),
        Field.of("name", StandardSQLTypeName.STRING),
        Field.of("raw_provided_id", StandardSQLTypeName.STRING),
        Field.of("content_language", StandardSQLTypeName.STRING),
        Field.of("feed_label", StandardSQLTypeName.STRING),
        getCssProductsAttributesField(),
        getCssProductsCssProductStatusField());
  }

  public BigQueryService(AccountInfo accountInfo) throws IOException {
    GoogleCredentials googleCredentials = new Authenticator().authenticate(accountInfo);
    BigQueryOptions bigQueryOptions =
        BigQueryOptions.newBuilder().setCredentials(googleCredentials).build();
    this.bigQuery = bigQueryOptions.getService();
  }
}
