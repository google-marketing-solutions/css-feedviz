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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.SettableApiFuture;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.Dataset;
import com.google.cloud.bigquery.DatasetId;
import com.google.cloud.bigquery.DatasetInfo;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Field.Mode;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.bigquery.StandardTableDefinition;
import com.google.cloud.bigquery.Table;
import com.google.cloud.bigquery.TableId;
import com.google.cloud.bigquery.TableInfo;
import com.google.cloud.bigquery.TimePartitioning;
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableSchema;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.cssfeedviz.utils.AccountInfo;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.google.shopping.css.v1.Attributes;
import com.google.shopping.css.v1.CssProduct;
import com.google.shopping.css.v1.CssProductStatus;
import com.google.shopping.css.v1.CssProductStatus.ItemLevelIssue;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.json.JSONArray;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

public class BigQueryServiceTest {

  private final String TEST_CONFIG_DIR = "./config/test";
  private final String ACCOUNT_INFO_FILE_NAME = "account-info.json";
  private final String TEST_DATASET_NAME = "TEST_DATASET";
  private final String CSS_PRODUCTS_TABLE_NAME = "css_products";
  private final String TEST_TABLE_NAME = "css_products";
  private final String TEST_LOCATION = "EU";
  private final String PRODUCT_NAME = "Test Product Name";
  private final String WRITE_STREAM_NAME =
      String.format(
          "projects/google.com:test-project/datasets/%1$s/tables/%2$s/streams/TEST_STREAM",
          TEST_DATASET_NAME, TEST_TABLE_NAME);
  private final LocalDateTime TEST_TRANSFER_DATE = LocalDateTime.now();
  private final CssProduct CSS_PRODUCT = CssProduct.newBuilder().setName(PRODUCT_NAME).build();
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
          Field.of("transfer_date", StandardSQLTypeName.TIMESTAMP),
          Field.of("name", StandardSQLTypeName.STRING),
          Field.of("raw_provided_id", StandardSQLTypeName.STRING),
          Field.of("content_language", StandardSQLTypeName.STRING),
          Field.of("feed_label", StandardSQLTypeName.STRING),
          CSS_PRODUCTS_ATTRIBUTES_FIELD,
          CSS_PRODUCTS_CSS_PRODUCT_STATUS_FIELD);
  private final Map<String, String> TEST_PRICE_MAP =
      Map.of(
          "amount_micros",
          String.valueOf(CSS_PRODUCT.getAttributes().getLowPrice().getAmountMicros()),
          "currency_code",
          CSS_PRODUCT.getAttributes().getLowPrice().getCurrencyCode());
  private final Map<String, Object> TEST_PRODUCT_DIMENSION_MAP =
      Map.of(
          "value",
          CSS_PRODUCT.getAttributes().getProductHeight().getValue(),
          "unit",
          CSS_PRODUCT.getAttributes().getProductHeight().getUnit());

  private AccountInfo accountInfo;
  private BigQueryService bigQueryService;
  private MockedStatic<BigQueryWriteClient> mockedStaticBigQueryWriteClient;
  private MockedStatic<JsonStreamWriter> mockedStaticJsonStreamWriter;
  private BigQueryWriteClient mockBigQueryWriteClient;
  private JsonStreamWriter mockJsonStreamWriter;
  private WriteStream mockWriteStream;

  @Mock private BigQuery mockBigQuery;
  @Mock private Dataset mockDataset;
  @Mock private Table mockTable;
  @Mock private InsertAllResponse mockInsertAllResponse;

  @Before
  public void setUp()
      throws IOException,
          IllegalArgumentException,
          DescriptorValidationException,
          InterruptedException {
    MockitoAnnotations.openMocks(this);

    accountInfo = AccountInfo.load(TEST_CONFIG_DIR, ACCOUNT_INFO_FILE_NAME);
    bigQueryService = new BigQueryService(accountInfo);
    bigQueryService.setBigQuery(mockBigQuery);

    mockBigQueryWriteClient = mock(BigQueryWriteClient.class);
    mockWriteStream = mock(WriteStream.class);

    mockedStaticBigQueryWriteClient = mockStatic(BigQueryWriteClient.class);
    mockedStaticBigQueryWriteClient
        .when(BigQueryWriteClient::create)
        .thenReturn(mockBigQueryWriteClient);

    when(mockBigQueryWriteClient.createWriteStream(any(CreateWriteStreamRequest.class)))
        .thenReturn(mockWriteStream);
    when(mockWriteStream.getName()).thenReturn(WRITE_STREAM_NAME);
    when(mockWriteStream.getTableSchema()).thenReturn(TableSchema.newBuilder().build());
    when(mockWriteStream.getLocation()).thenReturn(TEST_LOCATION);

    JsonStreamWriter.Builder mockJsonStreamWriterBuilder = mock(JsonStreamWriter.Builder.class);
    mockJsonStreamWriter = mock(JsonStreamWriter.class);

    mockedStaticJsonStreamWriter = mockStatic(JsonStreamWriter.class);
    mockedStaticJsonStreamWriter
        .when(
            () ->
                JsonStreamWriter.newBuilder(
                    anyString(), any(TableSchema.class), any(BigQueryWriteClient.class)))
        .thenReturn(mockJsonStreamWriterBuilder);

    when(mockJsonStreamWriterBuilder.build()).thenReturn(mockJsonStreamWriter);
    when(mockBigQuery.create(DATASET_INFO)).thenReturn(mockDataset);
  }

  @After
  public void tearDown() {
    mockedStaticBigQueryWriteClient.close();
    mockedStaticJsonStreamWriter.close();
  }

  @Test
  public void datasetExists_datasetExists() {
    when(mockBigQuery.getDataset(DATASET_ID)).thenReturn(mockDataset);
    assertTrue(bigQueryService.datasetExists(TEST_DATASET_NAME));
  }

  @Test
  public void datasetExists_datasetDoesNotExist() {
    when(mockBigQuery.getDataset(DATASET_ID)).thenReturn(null);
    assertFalse(bigQueryService.datasetExists(TEST_DATASET_NAME));
  }

  @Test
  public void createDataset() throws IOException {
    when(mockBigQuery.create(DATASET_INFO)).thenReturn(mockDataset);
    assertEquals(mockDataset, bigQueryService.createDataset(TEST_DATASET_NAME, TEST_LOCATION));
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
    when(mockBigQuery.getTable(TABLE_ID)).thenReturn(mockTable);
    assertTrue(bigQueryService.tableExists(TEST_DATASET_NAME, TEST_TABLE_NAME));
  }

  @Test
  public void tableExists_tableDoesNotExist() {
    when(mockBigQuery.getTable(TABLE_ID)).thenReturn(null);
    assertFalse(bigQueryService.tableExists(TEST_DATASET_NAME, TEST_TABLE_NAME));
  }

  @Test
  public void createCssProductsTable() {
    TableId tableId = TableId.of(TEST_DATASET_NAME, CSS_PRODUCTS_TABLE_NAME);
    long thirtyDaysInMs = 2592000000L;
    TimePartitioning timePartitioning =
        TimePartitioning.newBuilder(TimePartitioning.Type.HOUR)
            .setField("transfer_date")
            .setExpirationMs(thirtyDaysInMs)
            .build();
    StandardTableDefinition tableDefinition =
        StandardTableDefinition.newBuilder()
            .setSchema(CSS_PRODUCTS_SCHEMA)
            .setTimePartitioning(timePartitioning)
            .build();
    TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();

    when(mockBigQuery.create(tableInfo)).thenReturn(mockTable);
    assertEquals(mockTable, bigQueryService.createCssProductsTable(TEST_DATASET_NAME));
  }

  @Test
  public void getPriceAsMap() {
    assertEquals(
        TEST_PRICE_MAP, bigQueryService.getPriceAsMap(CSS_PRODUCT.getAttributes().getLowPrice()));
  }

  @Test
  public void getProductDimensionAsMap() {
    assertEquals(
        TEST_PRODUCT_DIMENSION_MAP,
        bigQueryService.getProductDimensionAsMap(CSS_PRODUCT.getAttributes().getProductHeight()));
  }

  @Test
  public void getItemLevelIssueAsMap() {
    String testDescription = "Test Description";
    ItemLevelIssue itemLevelIssue =
        ItemLevelIssue.newBuilder().setDescription(testDescription).build();
    Map<String, Object> itemLevelIssueMap = new HashMap<String, Object>();
    itemLevelIssueMap.put("code", itemLevelIssue.getCode());
    itemLevelIssueMap.put("servability", itemLevelIssue.getServability());
    itemLevelIssueMap.put("resolution", itemLevelIssue.getResolution());
    itemLevelIssueMap.put("attribute", itemLevelIssue.getAttribute());
    itemLevelIssueMap.put("destination", itemLevelIssue.getDestination());
    itemLevelIssueMap.put("description", itemLevelIssue.getDescription());
    itemLevelIssueMap.put("detail", itemLevelIssue.getDetail());
    itemLevelIssueMap.put("documentation", itemLevelIssue.getDocumentation());
    itemLevelIssueMap.put("applicable_countries", itemLevelIssue.getApplicableCountriesList());

    assertEquals(itemLevelIssueMap, bigQueryService.getItemLevelIssueAsMap(itemLevelIssue));
  }

  @Test
  public void getTimestampAsString_returnsTimestampString() {
    long secondsSinceEpoch = TEST_TRANSFER_DATE.atZone(ZoneId.systemDefault()).toEpochSecond();
    Timestamp timestamp = Timestamp.newBuilder().setSeconds(secondsSinceEpoch).build();
    String timestampString = Timestamps.toString(timestamp);

    assertEquals(timestampString, bigQueryService.getTimestampAsString(timestamp));
  }

  @Test
  public void getTimestampAsString_defaultTimestampInstance_returnsNull() {
    assertEquals(null, bigQueryService.getTimestampAsString(Timestamp.getDefaultInstance()));
  }

  @Test
  public void testGetCssProductAsMap() {
    Attributes cssProductAttributes = CSS_PRODUCT.getAttributes();

    Map<String, Object> testAttributes = new HashMap<String, Object>();
    testAttributes.put("low_price", TEST_PRICE_MAP);
    testAttributes.put("high_price", TEST_PRICE_MAP);
    testAttributes.put("headline_offer_price", TEST_PRICE_MAP);
    testAttributes.put("headline_offer_shipping_price", TEST_PRICE_MAP);
    testAttributes.put(
        "additional_image_links", cssProductAttributes.getAdditionalImageLinksList());
    testAttributes.put("product_types", cssProductAttributes.getProductTypesList());
    testAttributes.put("size_types", cssProductAttributes.getSizeTypesList());
    testAttributes.put("product_details", cssProductAttributes.getProductDetailsList());
    testAttributes.put("product_weight", TEST_PRODUCT_DIMENSION_MAP);
    testAttributes.put("product_width", TEST_PRODUCT_DIMENSION_MAP);
    testAttributes.put("product_height", TEST_PRODUCT_DIMENSION_MAP);
    testAttributes.put("product_length", TEST_PRODUCT_DIMENSION_MAP);
    testAttributes.put("product_highlights", cssProductAttributes.getProductHighlightsList());
    testAttributes.put("certifications", cssProductAttributes.getCertificationsList());
    testAttributes.put(
        "expiration_date",
        bigQueryService.getTimestampAsString(cssProductAttributes.getExpirationDate()));
    testAttributes.put("included_destinations", cssProductAttributes.getIncludedDestinationsList());
    testAttributes.put("excluded_destinations", cssProductAttributes.getExcludedDestinationsList());
    testAttributes.put("cpp_link", cssProductAttributes.getCppLink());
    testAttributes.put("cpp_mobile_link", cssProductAttributes.getCppMobileLink());
    testAttributes.put("cpp_ads_redirect", cssProductAttributes.getCppAdsRedirect());
    testAttributes.put("number_of_offers", cssProductAttributes.getNumberOfOffers());
    testAttributes.put(
        "headline_offer_condition", cssProductAttributes.getHeadlineOfferCondition());
    testAttributes.put("headline_offer_link", cssProductAttributes.getHeadlineOfferLink());
    testAttributes.put(
        "headline_offer_mobile_link", cssProductAttributes.getHeadlineOfferMobileLink());
    testAttributes.put("title", cssProductAttributes.getTitle());
    testAttributes.put("image_link", cssProductAttributes.getImageLink());
    testAttributes.put("description", cssProductAttributes.getDescription());
    testAttributes.put("brand", cssProductAttributes.getBrand());
    testAttributes.put("mpn", cssProductAttributes.getMpn());
    testAttributes.put("gtin", cssProductAttributes.getGtin());
    testAttributes.put("google_product_category", cssProductAttributes.getGoogleProductCategory());
    testAttributes.put("adult", cssProductAttributes.getAdult());
    testAttributes.put("multipack", cssProductAttributes.getMultipack());
    testAttributes.put("is_bundle", cssProductAttributes.getIsBundle());
    testAttributes.put("age_group", cssProductAttributes.getAgeGroup());
    testAttributes.put("color", cssProductAttributes.getColor());
    testAttributes.put("gender", cssProductAttributes.getGender());
    testAttributes.put("material", cssProductAttributes.getMaterial());
    testAttributes.put("pattern", cssProductAttributes.getPattern());
    testAttributes.put("size", cssProductAttributes.getSize());
    testAttributes.put("size_system", cssProductAttributes.getSizeSystem());
    testAttributes.put("item_group_id", cssProductAttributes.getItemGroupId());
    testAttributes.put("pause", cssProductAttributes.getPause());
    testAttributes.put("custom_label_0", cssProductAttributes.getCustomLabel0());
    testAttributes.put("custom_label_1", cssProductAttributes.getCustomLabel1());
    testAttributes.put("custom_label_2", cssProductAttributes.getCustomLabel2());
    testAttributes.put("custom_label_3", cssProductAttributes.getCustomLabel3());
    testAttributes.put("custom_label_4", cssProductAttributes.getCustomLabel4());

    CssProductStatus cssProductStatus = CSS_PRODUCT.getCssProductStatus();
    Map<String, Object> testProductStatus = new HashMap<String, Object>();
    testProductStatus.put("destination_statuses", cssProductStatus.getDestinationStatusesList());
    testProductStatus.put("item_level_issues", cssProductStatus.getItemLevelIssuesList());
    testProductStatus.put(
        "creation_date", bigQueryService.getTimestampAsString(cssProductStatus.getCreationDate()));
    testProductStatus.put(
        "last_update_date",
        bigQueryService.getTimestampAsString(cssProductStatus.getLastUpdateDate()));
    testProductStatus.put(
        "google_expiration_date",
        bigQueryService.getTimestampAsString(cssProductStatus.getGoogleExpirationDate()));

    Map<String, Object> testRowContent = new HashMap<String, Object>();
    testRowContent.put("transfer_date", TEST_TRANSFER_DATE);
    testRowContent.put("name", CSS_PRODUCT.getName());
    testRowContent.put("raw_provided_id", CSS_PRODUCT.getRawProvidedId());
    testRowContent.put("content_language", CSS_PRODUCT.getContentLanguage());
    testRowContent.put("feed_label", CSS_PRODUCT.getFeedLabel());
    testRowContent.put("attributes", testAttributes);
    testRowContent.put("css_product_status", testProductStatus);

    assertEquals(
        testRowContent.toString(),
        bigQueryService.getCssProductAsMap(CSS_PRODUCT, TEST_TRANSFER_DATE).toString());
  }

  @Test
  public void testStreamCssProducts_EmptyProductsList()
      throws ExecutionException,
          InterruptedException,
          IOException,
          IllegalArgumentException,
          DescriptorValidationException {
    // Test with empty input
    List<CssProduct> cssProducts = List.of();

    // No errors should be thrown
    bigQueryService.streamCssProducts(
        TEST_DATASET_NAME, TEST_LOCATION, cssProducts, TEST_TRANSFER_DATE);
  }

  @Test
  public void testStreamCssProducts_SingleBatch()
      throws ExecutionException, InterruptedException, IOException, DescriptorValidationException {
    List<CssProduct> cssProducts = Arrays.asList(CSS_PRODUCT, CSS_PRODUCT, CSS_PRODUCT);

    // Prepare mock responses
    SettableApiFuture<AppendRowsResponse> successFuture = SettableApiFuture.create();
    successFuture.set(AppendRowsResponse.newBuilder().build());

    when(mockJsonStreamWriter.append(any(JSONArray.class), anyLong()))
        .thenReturn(successFuture); // Return the successful future

    bigQueryService.streamCssProducts(
        TEST_DATASET_NAME, TEST_LOCATION, cssProducts, TEST_TRANSFER_DATE);

    // Verify
    verify(mockJsonStreamWriter, times(1))
        .append(any(JSONArray.class), anyLong()); // Ensure append is called once
  }

  @Test
  public void testStreamCssProducts_MultiBatch()
      throws IOException,
          DescriptorValidationException,
          IllegalArgumentException,
          InterruptedException,
          ExecutionException {
    // Populate list of CSS products
    List<CssProduct> cssProducts = new ArrayList<CssProduct>();
    for (int i = 0; i < 500; i++) {
      cssProducts.add(CSS_PRODUCT);
    }

    // Prepare mock responses
    SettableApiFuture<AppendRowsResponse> successFuture = SettableApiFuture.create();
    successFuture.set(AppendRowsResponse.newBuilder().build());

    when(mockJsonStreamWriter.append(any(JSONArray.class), anyLong()))
        .thenReturn(successFuture); // Return the successful future

    bigQueryService.streamCssProducts(
        TEST_DATASET_NAME, TEST_LOCATION, cssProducts, TEST_TRANSFER_DATE);

    // Verify
    verify(mockJsonStreamWriter, times(5))
        .append(any(JSONArray.class), anyLong()); // Ensure append is called 5 times
  }

  @Test(expected = RuntimeException.class)
  public void testStreamCssProducts_FailedAppend()
      throws IllegalArgumentException,
          InterruptedException,
          ExecutionException,
          IOException,
          DescriptorValidationException {
    List<CssProduct> cssProducts = Arrays.asList(CSS_PRODUCT, CSS_PRODUCT, CSS_PRODUCT);

    SettableApiFuture<AppendRowsResponse> failureFuture = SettableApiFuture.create();
    failureFuture.setException(new IOException("Failed to append")); // Return failure future

    when(mockJsonStreamWriter.append(any(JSONArray.class), anyLong()))
        .thenReturn(failureFuture); // Return the failure future

    bigQueryService.streamCssProducts(
        TEST_DATASET_NAME, TEST_LOCATION, cssProducts, TEST_TRANSFER_DATE);
  }
}
