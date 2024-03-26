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
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
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
  private final String CSS_PRODUCTS_TABLE_NAME = "css_products";
  private final String TEST_TABLE_NAME = "css_products";
  private final String TEST_LOCATION = "EU";
  private final DatasetId DATASET_ID = DatasetId.of(TEST_DATASET_NAME);
  private final DatasetInfo DATASET_INFO =
      DatasetInfo.newBuilder(TEST_DATASET_NAME).setLocation(TEST_LOCATION).build();
  private final TableId TABLE_ID = TableId.of(TEST_DATASET_NAME, TEST_TABLE_NAME);
  private final Field PRICE_AMOUNT_MICROS = Field.of("amount_micros", StandardSQLTypeName.INT64);
  private final Field PRICE_CURRENCY_CODE = Field.of("currency_code", StandardSQLTypeName.STRING);
  private final Field PRODUCT_DIMENSION_VALUE = Field.of("value", StandardSQLTypeName.FLOAT64);
  private final Field PRODUCT_DIMENSION_UNIT = Field.of("unit", StandardSQLTypeName.STRING);
  private final Field CSS_PRODUCTS_ATTRIBUTES_FIELD =
      Field.of(
          "attributes",
          StandardSQLTypeName.STRUCT,
          Field.of(
              "low_price", StandardSQLTypeName.STRUCT, PRICE_AMOUNT_MICROS, PRICE_CURRENCY_CODE),
          Field.of(
              "high_price", StandardSQLTypeName.STRUCT, PRICE_AMOUNT_MICROS, PRICE_CURRENCY_CODE),
          Field.of(
              "headline_offer_price",
              StandardSQLTypeName.STRUCT,
              PRICE_AMOUNT_MICROS,
              PRICE_CURRENCY_CODE),
          Field.of(
              "headline_offer_shipping_price",
              StandardSQLTypeName.STRUCT,
              PRICE_AMOUNT_MICROS,
              PRICE_CURRENCY_CODE),
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
              PRODUCT_DIMENSION_VALUE,
              PRODUCT_DIMENSION_UNIT),
          Field.of(
              "product_length",
              StandardSQLTypeName.STRUCT,
              PRODUCT_DIMENSION_VALUE,
              PRODUCT_DIMENSION_UNIT),
          Field.of(
              "product_width",
              StandardSQLTypeName.STRUCT,
              PRODUCT_DIMENSION_VALUE,
              PRODUCT_DIMENSION_UNIT),
          Field.of(
              "product_height",
              StandardSQLTypeName.STRUCT,
              PRODUCT_DIMENSION_VALUE,
              PRODUCT_DIMENSION_UNIT),
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
  private final Field CSS_PRODUCTS_CSS_PRODUCT_STATUS_FIELD =
      Field.of(
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
  private final Schema CSS_PRODUCTS_SCHEMA =
      Schema.of(
          Field.of("date", StandardSQLTypeName.DATE),
          Field.of("name", StandardSQLTypeName.STRING),
          Field.of("raw_provided_id", StandardSQLTypeName.STRING),
          Field.of("content_language", StandardSQLTypeName.STRING),
          Field.of("feed_label", StandardSQLTypeName.STRING),
          CSS_PRODUCTS_ATTRIBUTES_FIELD,
          CSS_PRODUCTS_CSS_PRODUCT_STATUS_FIELD);

  private AccountInfo accountInfo;
  private BigQueryService bigQueryService;

  @Mock private BigQuery bigQuery;
  @Mock private Dataset dataset;
  @Mock private Table table;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    accountInfo = AccountInfo.load(TEST_CONFIG_DIR, ACCOUNT_INFO_FILE_NAME);
    bigQueryService = new BigQueryService(accountInfo);
    bigQueryService.setBigQuery(bigQuery);
  }

  @Test
  public void datasetExists_datasetExists() {
    when(bigQuery.getDataset(DATASET_ID)).thenReturn(dataset);
    assertTrue(bigQueryService.datasetExists(TEST_DATASET_NAME));
  }

  @Test
  public void datasetExists_datasetDoesNotExist() {
    when(bigQuery.getDataset(DATASET_ID)).thenReturn(null);
    assertFalse(bigQueryService.datasetExists(TEST_DATASET_NAME));
  }

  @Test
  public void createDataset() throws IOException {
    when(bigQuery.create(DATASET_INFO)).thenReturn(dataset);
    assertEquals(dataset, bigQueryService.createDataset(TEST_DATASET_NAME, TEST_LOCATION));
  }

  @Test
  public void getCssProductsAttributesField() {
    assertEquals(CSS_PRODUCTS_ATTRIBUTES_FIELD, bigQueryService.getCssProductsAttributesField());
  }

  @Test
  public void getCssProductsCssProductStatusField() {
    assertEquals(
        CSS_PRODUCTS_CSS_PRODUCT_STATUS_FIELD,
        bigQueryService.getCssProductsCssProductStatusField());
  }

  @Test
  public void getCssProductsSchema() {
    assertEquals(CSS_PRODUCTS_SCHEMA, bigQueryService.getCssProductsSchema());
  }

  @Test
  public void tableExists_tableExists() {
    when(bigQuery.getTable(TABLE_ID)).thenReturn(table);
    assertTrue(bigQueryService.tableExists(TEST_DATASET_NAME, TEST_TABLE_NAME));
  }

  @Test
  public void tableExists_tableDoesNotExist() {
    when(bigQuery.getTable(TABLE_ID)).thenReturn(null);
    assertFalse(bigQueryService.tableExists(TEST_DATASET_NAME, TEST_TABLE_NAME));
  }

  @Test
  public void createCssProductsTable() {
    TableId tableId = TableId.of(TEST_DATASET_NAME, CSS_PRODUCTS_TABLE_NAME);
    long ninetyDaysInMs = 7776000000L;
    TimePartitioning timePartitioning =
        TimePartitioning.newBuilder(TimePartitioning.Type.DAY)
            .setField("date")
            .setExpirationMs(ninetyDaysInMs)
            .build();
    StandardTableDefinition tableDefinition =
        StandardTableDefinition.newBuilder()
            .setSchema(CSS_PRODUCTS_SCHEMA)
            .setTimePartitioning(timePartitioning)
            .build();
    TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

    when(bigQuery.create(tableInfo)).thenReturn(table);
    assertEquals(table, bigQueryService.createCssProductsTable(TEST_DATASET_NAME));
  }
}
