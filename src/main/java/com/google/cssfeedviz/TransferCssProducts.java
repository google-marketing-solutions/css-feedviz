package com.google.cssfeedviz;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cssfeedviz.css.ProductsService;
import com.google.cssfeedviz.gcp.BigQueryService;
import com.google.cssfeedviz.utils.AccountInfo;
import com.google.shopping.css.v1.CssProduct;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;

public class TransferCssProducts {
  private static final String DEFAULT_CONFIG_DIR = "./config";
  private static final String DEFAULT_ACCOUNT_INFO_FILE = "account-info.json";
  private static final String DEFAULT_DATASET_NAME = "css_feedviz";
  private static final String DEFAULT_DATASET_LOCATION = "EU";

  private static final String CONFIG_DIR =
      System.getProperty("feedviz.config.dir", DEFAULT_CONFIG_DIR);
  private static String ACCOUNT_INFO_FILE =
      System.getProperty("feedviz.account.info.file", DEFAULT_ACCOUNT_INFO_FILE);
  private static String DATASET_NAME =
      System.getProperty("feedviz.dataset.name", DEFAULT_DATASET_NAME);
  private static String DATASET_LOCATION =
      System.getProperty("feedviz.dataset.location", DEFAULT_DATASET_LOCATION);

  public static void main(String[] args) {
    try {
      AccountInfo accountInfo = AccountInfo.load(CONFIG_DIR, ACCOUNT_INFO_FILE);
      ProductsService productsService = new ProductsService(accountInfo);
      Iterable<CssProduct> cssProducts = productsService.listCssProducts();

      BigQueryService bigQueryService = new BigQueryService(accountInfo);
      InsertAllResponse insertAllResponse =
          bigQueryService.insertCssProducts(
              DATASET_NAME, DATASET_LOCATION, cssProducts, LocalDateTime.now());

      if (insertAllResponse.hasErrors()) System.out.println("Big Query Insert Errors:");
      for (List<BigQueryError> bigQueryErrors : insertAllResponse.getInsertErrors().values()) {
        for (BigQueryError bigQueryError : bigQueryErrors) {
          System.out.println(bigQueryError.toString());
        }
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
