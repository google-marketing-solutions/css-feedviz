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

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.BigQueryOptions;
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
import com.google.cloud.bigquery.storage.v1.AppendRowsResponse;
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient;
import com.google.cloud.bigquery.storage.v1.CreateWriteStreamRequest;
import com.google.cloud.bigquery.storage.v1.Exceptions;
import com.google.cloud.bigquery.storage.v1.Exceptions.StorageException;
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter;
import com.google.cloud.bigquery.storage.v1.TableName;
import com.google.cloud.bigquery.storage.v1.WriteStream;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.cssfeedviz.utils.AccountInfo;
import com.google.cssfeedviz.utils.Authenticator;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.protobuf.Timestamp;
import com.google.protobuf.util.Timestamps;
import com.google.shopping.css.v1.Attributes;
import com.google.shopping.css.v1.CssProduct;
import com.google.shopping.css.v1.CssProductStatus;
import com.google.shopping.css.v1.CssProductStatus.ItemLevelIssue;
import com.google.shopping.css.v1.ProductDimension;
import com.google.shopping.css.v1.ProductWeight;
import com.google.shopping.type.Price;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import javax.annotation.concurrent.GuardedBy;
import org.json.JSONArray;

public class BigQueryService {
  private final String CSS_PRODUCTS_TABLE_NAME = "css_products";
  private final int INSERT_BATCH_SIZE = 100;

  private BigQuery bigQuery;
  private ServiceAccountCredentials serviceAccountCredentials;
  private final Object lock = new Object();

  @GuardedBy("lock")
  private RuntimeException error = null;

  public void setBigQuery(BigQuery bigQuery) {
    this.bigQuery = bigQuery;
  }

  public boolean datasetExists(String datasetName) {
    Dataset dataset = this.bigQuery.getDataset(DatasetId.of(datasetName));
    return dataset != null;
  }

  public boolean tableExists(String datasetName, String tableName) {
    Table table = this.bigQuery.getTable(TableId.of(datasetName, tableName));
    return table != null;
  }

  public Dataset createDataset(String datasetName, String location) {
    DatasetInfo datasetInfo = DatasetInfo.newBuilder(datasetName).setLocation(location).build();
    return this.bigQuery.create(datasetInfo);
  }

  public Table createCssProductsTable(String datasetName) {
    TableId tableId = TableId.of(datasetName, CSS_PRODUCTS_TABLE_NAME);
    long thirtyDaysInMs = 2592000000L;
    TimePartitioning timePartitioning =
        TimePartitioning.newBuilder(TimePartitioning.Type.HOUR)
            .setField("transfer_date")
            .setExpirationMs(thirtyDaysInMs)
            .build();
    StandardTableDefinition tableDefinition =
        StandardTableDefinition.newBuilder()
            .setSchema(getCssProductsSchema())
            .setTimePartitioning(timePartitioning)
            .build();
    TableInfo tableInfo = TableInfo.newBuilder(tableId, tableDefinition).build();
    return bigQuery.create(tableInfo);
  }

  public Map<String, String> getPriceAsMap(Price price) {
    return Map.of(
        "amount_micros", String.valueOf(price.getAmountMicros()),
        "currency_code", price.getCurrencyCode());
  }

  public Map<String, Object> getProductDimensionAsMap(ProductDimension productDimension) {
    return Map.of("value", productDimension.getValue(), "unit", productDimension.getUnit());
  }

  public String getTimestampAsString(Timestamp timestamp) {
    if (timestamp == Timestamp.getDefaultInstance()) {
      return null;
    }
    return Timestamps.toString(timestamp);
  }

  public Map<String, Object> getItemLevelIssueAsMap(ItemLevelIssue itemLevelIssue) {
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
    return itemLevelIssueMap;
  }

  public Map<String, Object> getCssProductAsMap(CssProduct cssProduct, LocalDateTime transferDate) {
    Attributes cssProductAttributes = cssProduct.getAttributes();

    List<Map<String, String>> productDetailsList =
        cssProductAttributes.getProductDetailsList().stream()
            .map(
                productDetails ->
                    Map.of(
                        "section_name", productDetails.getSectionName(),
                        "attribute_name", productDetails.getAttributeName(),
                        "attribute_value", productDetails.getAttributeValue()))
            .collect(Collectors.toList());
    ProductWeight cssProductWeight = cssProductAttributes.getProductWeight();
    Map<String, Object> productWeightMap =
        Map.of("value", cssProductWeight.getValue(), "unit", cssProductWeight.getUnit());
    List<Map<String, String>> certificationsList =
        cssProductAttributes.getCertificationsList().stream()
            .map(
                certification ->
                    Map.of(
                        "name",
                        certification.getName(),
                        "authority",
                        certification.getAuthority(),
                        "code",
                        certification.getCode()))
            .collect(Collectors.toList());

    Map<String, Object> attributesMap = new HashMap<String, Object>();
    attributesMap.put("low_price", getPriceAsMap(cssProductAttributes.getLowPrice()));
    attributesMap.put("high_price", getPriceAsMap(cssProductAttributes.getHighPrice()));
    attributesMap.put(
        "headline_offer_price", getPriceAsMap(cssProductAttributes.getHeadlineOfferPrice()));
    attributesMap.put(
        "headline_offer_shipping_price",
        getPriceAsMap(cssProductAttributes.getHeadlineOfferShippingPrice()));
    attributesMap.put("additional_image_links", cssProductAttributes.getAdditionalImageLinksList());
    attributesMap.put("product_types", cssProductAttributes.getProductTypesList());
    attributesMap.put("size_types", cssProductAttributes.getSizeTypesList());
    attributesMap.put("product_details", productDetailsList);
    attributesMap.put("product_weight", productWeightMap);
    attributesMap.put(
        "product_width", getProductDimensionAsMap(cssProductAttributes.getProductWidth()));
    attributesMap.put(
        "product_height", getProductDimensionAsMap(cssProductAttributes.getProductHeight()));
    attributesMap.put(
        "product_length", getProductDimensionAsMap(cssProductAttributes.getProductLength()));
    attributesMap.put("product_highlights", cssProductAttributes.getProductHighlightsList());
    attributesMap.put("certifications", certificationsList);
    attributesMap.put(
        "expiration_date", getTimestampAsString(cssProductAttributes.getExpirationDate()));
    attributesMap.put("included_destinations", cssProductAttributes.getIncludedDestinationsList());
    attributesMap.put("excluded_destinations", cssProductAttributes.getExcludedDestinationsList());
    attributesMap.put("cpp_link", cssProductAttributes.getCppLink());
    attributesMap.put("cpp_mobile_link", cssProductAttributes.getCppMobileLink());
    attributesMap.put("cpp_ads_redirect", cssProductAttributes.getCppAdsRedirect());
    attributesMap.put("number_of_offers", cssProductAttributes.getNumberOfOffers());
    attributesMap.put("headline_offer_condition", cssProductAttributes.getHeadlineOfferCondition());
    attributesMap.put("headline_offer_link", cssProductAttributes.getHeadlineOfferLink());
    attributesMap.put(
        "headline_offer_mobile_link", cssProductAttributes.getHeadlineOfferMobileLink());
    attributesMap.put("title", cssProductAttributes.getTitle());
    attributesMap.put("image_link", cssProductAttributes.getImageLink());
    attributesMap.put("description", cssProductAttributes.getDescription());
    attributesMap.put("brand", cssProductAttributes.getBrand());
    attributesMap.put("mpn", cssProductAttributes.getMpn());
    attributesMap.put("gtin", cssProductAttributes.getGtin());
    attributesMap.put("google_product_category", cssProductAttributes.getGoogleProductCategory());
    attributesMap.put("adult", cssProductAttributes.getAdult());
    attributesMap.put("multipack", String.valueOf(cssProductAttributes.getMultipack()));
    attributesMap.put("is_bundle", cssProductAttributes.getIsBundle());
    attributesMap.put("age_group", cssProductAttributes.getAgeGroup());
    attributesMap.put("color", cssProductAttributes.getColor());
    attributesMap.put("gender", cssProductAttributes.getGender());
    attributesMap.put("material", cssProductAttributes.getMaterial());
    attributesMap.put("pattern", cssProductAttributes.getPattern());
    attributesMap.put("size", cssProductAttributes.getSize());
    attributesMap.put("size_system", cssProductAttributes.getSizeSystem());
    attributesMap.put("item_group_id", cssProductAttributes.getItemGroupId());
    attributesMap.put("pause", cssProductAttributes.getPause());
    attributesMap.put("custom_label_0", cssProductAttributes.getCustomLabel0());
    attributesMap.put("custom_label_1", cssProductAttributes.getCustomLabel1());
    attributesMap.put("custom_label_2", cssProductAttributes.getCustomLabel2());
    attributesMap.put("custom_label_3", cssProductAttributes.getCustomLabel3());
    attributesMap.put("custom_label_4", cssProductAttributes.getCustomLabel4());

    CssProductStatus cssProductStatus = cssProduct.getCssProductStatus();
    List<Map<String, Object>> destinationStatusList =
        cssProductStatus.getDestinationStatusesList().stream()
            .map(
                destinationStatus ->
                    Map.of(
                        "destination",
                        destinationStatus.getDestination(),
                        "approved_countries",
                        destinationStatus.getApprovedCountriesList(),
                        "pending_countries",
                        destinationStatus.getPendingCountriesList()))
            .collect(Collectors.toList());
    List<Map<String, Object>> itemLevelIssueList =
        cssProductStatus.getItemLevelIssuesList().stream()
            .map(itemLevelIssue -> getItemLevelIssueAsMap(itemLevelIssue))
            .collect(Collectors.toList());
    Map<String, Object> cssProductStatusMap = new HashMap<String, Object>();
    cssProductStatusMap.put("destination_statuses", destinationStatusList);
    cssProductStatusMap.put("item_level_issues", itemLevelIssueList);
    cssProductStatusMap.put(
        "creation_date", getTimestampAsString(cssProductStatus.getCreationDate()));
    cssProductStatusMap.put(
        "last_update_date", getTimestampAsString(cssProductStatus.getLastUpdateDate()));
    cssProductStatusMap.put(
        "google_expiration_date", getTimestampAsString(cssProductStatus.getGoogleExpirationDate()));

    Map<String, Object> rowContent = new HashMap<String, Object>();
    rowContent.put("transfer_date", transferDate.toString());
    rowContent.put("name", cssProduct.getName());
    rowContent.put("raw_provided_id", cssProduct.getRawProvidedId());
    rowContent.put("content_language", cssProduct.getContentLanguage());
    rowContent.put("feed_label", cssProduct.getFeedLabel());
    rowContent.put("attributes", attributesMap);
    rowContent.put("css_product_status", cssProductStatusMap);
    return rowContent;
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
        Field.of("transfer_date", StandardSQLTypeName.TIMESTAMP),
        Field.of("name", StandardSQLTypeName.STRING),
        Field.of("raw_provided_id", StandardSQLTypeName.STRING),
        Field.of("content_language", StandardSQLTypeName.STRING),
        Field.of("feed_label", StandardSQLTypeName.STRING),
        getCssProductsAttributesField(),
        getCssProductsCssProductStatusField());
  }

  public void streamCssProducts(
      String datasetName,
      String datasetLocation,
      Iterable<CssProduct> cssProducts,
      LocalDateTime transferDate)
      throws InterruptedException,
          ExecutionException,
          IOException,
          IllegalArgumentException,
          DescriptorValidationException {

    if (!datasetExists(datasetName)) createDataset(datasetName, datasetLocation);
    if (!tableExists(datasetName, CSS_PRODUCTS_TABLE_NAME)) createCssProductsTable(datasetName);

    TableId tableId =
        TableId.of(
            this.serviceAccountCredentials.getProjectId(), datasetName, CSS_PRODUCTS_TABLE_NAME);

    BigQueryWriteClient writeClient = BigQueryWriteClient.create();
    WriteStream writeStream = createWriteStream(writeClient, tableId);
    JsonStreamWriter streamWriter =
        JsonStreamWriter.newBuilder(
                writeStream.getName(), writeStream.getTableSchema(), writeClient)
            .build();
    long offset = 0;
    for (List<CssProduct> batch : Iterables.partition(cssProducts, INSERT_BATCH_SIZE)) {
      List<Map<String, Object>> batchRows =
          batch.stream().map(cssProduct -> getCssProductAsMap(cssProduct, transferDate)).toList();
      JSONArray jsonArray = new JSONArray(batchRows);
      ApiFuture<AppendRowsResponse> future = streamWriter.append(jsonArray, offset);
      ApiFutures.addCallback(
          future,
          new BigQueryService.AppendRowsCompleteCallback(this),
          MoreExecutors.directExecutor());
      offset += jsonArray.length();
    }

    streamWriter.close();
    writeClient.close();
    synchronized (this.lock) {
      if (this.error != null) {
        throw this.error;
      }
    }
  }

  private WriteStream createWriteStream(BigQueryWriteClient writeClient, TableId tableId) {
    WriteStream stream = WriteStream.newBuilder().setType(WriteStream.Type.COMMITTED).build();
    TableName parentTable =
        TableName.of(tableId.getProject(), tableId.getDataset(), tableId.getTable());
    CreateWriteStreamRequest createWriteStreamRequest =
        CreateWriteStreamRequest.newBuilder()
            .setParent(parentTable.toString())
            .setWriteStream(stream)
            .build();
    return writeClient.createWriteStream(createWriteStreamRequest);
  }

  public BigQueryService(AccountInfo accountInfo) throws IOException {

    this.serviceAccountCredentials =
        (ServiceAccountCredentials) new Authenticator().authenticate(accountInfo);
    BigQueryOptions bigQueryOptions =
        BigQueryOptions.newBuilder().setCredentials(this.serviceAccountCredentials).build();
    this.bigQuery = bigQueryOptions.getService();
  }

  static class AppendRowsCompleteCallback implements ApiFutureCallback<AppendRowsResponse> {
    private final BigQueryService parent;

    AppendRowsCompleteCallback(BigQueryService parent) {
      this.parent = parent;
    }

    @Override
    public void onFailure(Throwable throwable) {
      synchronized (this.parent.lock) {
        if (this.parent.error == null) {
          StorageException storageException = Exceptions.toStorageException(throwable);
          this.parent.error =
              (storageException != null) ? storageException : new RuntimeException(throwable);
        }
      }
      System.out.format("Error: %s\n", throwable.toString());
    }

    @Override
    public void onSuccess(AppendRowsResponse appendRowsResponse) {
      System.out.format(
          "Append %d success\n", appendRowsResponse.getAppendResult().getOffset().getValue());
    }
  }
}
