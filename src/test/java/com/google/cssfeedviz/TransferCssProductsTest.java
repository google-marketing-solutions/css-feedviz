package com.google.cssfeedviz;

import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cssfeedviz.css.ProductsService;
import com.google.cssfeedviz.gcp.BigQueryService;
import com.google.protobuf.Descriptors.DescriptorValidationException;
import com.google.shopping.css.v1.CssProduct;
import java.io.IOException;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

public class TransferCssProductsTest {
  private final String TEST_PRODUCT_NAME = "Test Product Name";
  private final String TEST_CONFIG_DIR = "./config/test";
  private final String TEST_DATASET_NAME = "css_feedviz";
  private final String TEST_DATASET_LOCATION = "EU";
  private final String TEST_GROUP_ID = "123";
  private final String TEST_DOMAIN_ID = "456";
  private final String TEST_MERCHANT_ID = "789";
  private final CssProduct CSS_PRODUCT = CssProduct.newBuilder().setName(TEST_PRODUCT_NAME).build();
  private final List<CssProduct> CSS_PRODUCT_LIST = List.of(CSS_PRODUCT);
  private final LocalDateTime TEST_TRANSFER_DATE = LocalDateTime.now();

  private MockedConstruction<ProductsService> mockProductsServiceController;
  private MockedConstruction<BigQueryService> mockBigQueryServiceController;
  private MockedStatic<LocalDateTime> mockedStaticLocalDateTime;

  @Before
  public void setUp() {
    System.setProperty("feedviz.config.dir", TEST_CONFIG_DIR);

    mockedStaticLocalDateTime = mockStatic(LocalDateTime.class);
    mockedStaticLocalDateTime.when(LocalDateTime::now).thenReturn(TEST_TRANSFER_DATE);

    mockProductsServiceController =
        mockConstruction(
            ProductsService.class,
            (mock, context) -> {
              when(mock.listCssProducts()).thenReturn(CSS_PRODUCT_LIST);
            });
    mockBigQueryServiceController = mockConstruction(BigQueryService.class, (mock, context) -> {});
  }

  @After
  public void tearDown() {
    mockedStaticLocalDateTime.close();
    mockProductsServiceController.close();
    mockBigQueryServiceController.close();
  }

  @Test
  public void testMain()
      throws ExecutionException,
          InterruptedException,
          IOException,
          IllegalArgumentException,
          DescriptorValidationException {
    TransferCssProducts.main(null);
    ProductsService mockProductsService = mockProductsServiceController.constructed().get(0);
    verify(mockProductsService).listCssProducts();

    BigQueryService mockBigQueryService = mockBigQueryServiceController.constructed().get(0);
    verify(mockBigQueryService)
        .streamCssProducts(
            TEST_DATASET_NAME, TEST_DATASET_LOCATION, CSS_PRODUCT_LIST, TEST_TRANSFER_DATE);
  }

  @Test
  public void testMain_withAccountSystemPropertiesSet()
      throws ExecutionException,
          InterruptedException,
          IOException,
          IllegalArgumentException,
          DescriptorValidationException {
    System.setProperty("feedviz.account.info.domain.id", TEST_DOMAIN_ID);
    System.setProperty("feedviz.account.info.group.id", TEST_GROUP_ID);
    System.setProperty("feedviz.account.info.merchant.id", TEST_MERCHANT_ID);

    TransferCssProducts.main(null);
    ProductsService mockProductsService = mockProductsServiceController.constructed().get(0);
    verify(mockProductsService).listCssProducts();

    BigQueryService mockBigQueryService = mockBigQueryServiceController.constructed().get(0);
    verify(mockBigQueryService)
        .streamCssProducts(
            TEST_DATASET_NAME, TEST_DATASET_LOCATION, CSS_PRODUCT_LIST, TEST_TRANSFER_DATE);
  }
}
