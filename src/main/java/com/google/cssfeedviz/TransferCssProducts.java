package com.google.cssfeedviz;

import com.google.cssfeedviz.css.ProductsService;
import com.google.cssfeedviz.gcp.BigQueryService;
import com.google.cssfeedviz.utils.AccountInfo;
import com.google.shopping.css.v1.CssProduct;
import java.io.IOException;
import java.math.BigInteger;
import java.time.LocalDateTime;

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
  private static String ACCOUNT_INFO_DOMAIN_ID =
      System.getProperty("feedviz.account.info.domain.id");
  private static String ACCOUNT_INFO_GROUP_ID = System.getProperty("feedviz.account.info.group.id");
  private static String ACCOUNT_INFO_MERCHANT_ID =
      System.getProperty("feedviz.account.info.merchant.id");

  private static AccountInfo getAccountInfo() throws IOException {
    BigInteger domainId =
        (ACCOUNT_INFO_DOMAIN_ID != null) ? new BigInteger(ACCOUNT_INFO_DOMAIN_ID) : null;
    BigInteger groupId =
        (ACCOUNT_INFO_GROUP_ID != null) ? new BigInteger(ACCOUNT_INFO_GROUP_ID) : null;
    BigInteger merchantId =
        (ACCOUNT_INFO_MERCHANT_ID != null) ? new BigInteger(ACCOUNT_INFO_MERCHANT_ID) : null;
    if (domainId != null || groupId != null || merchantId != null) {
      return AccountInfo.create(merchantId, domainId, groupId);
    } else {
      return AccountInfo.load(CONFIG_DIR, ACCOUNT_INFO_FILE);
    }
  }

  public static void main(String[] args) {
    try {
      AccountInfo accountInfo = getAccountInfo();
      ProductsService productsService = ProductsService.create(accountInfo);
      Iterable<CssProduct> cssProducts = productsService.listCssProducts();

      BigQueryService bigQueryService = new BigQueryService(accountInfo);
      bigQueryService.streamCssProducts(
          DATASET_NAME, DATASET_LOCATION, cssProducts, LocalDateTime.now());
    } catch (Exception e) {
      System.err.println(e.getMessage());
      e.printStackTrace();
    }
  }
}
