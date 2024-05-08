package com.google.cssfeedviz;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.InsertAllResponse;
import com.google.cssfeedviz.css.ProductsService;
import com.google.cssfeedviz.gcp.BigQueryService;
import com.google.shopping.css.v1.CssProduct;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
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
  private final CssProduct CSS_PRODUCT = CssProduct.newBuilder().setName(TEST_PRODUCT_NAME).build();
  private final List<CssProduct> CSS_PRODUCT_LIST = List.of(CSS_PRODUCT);
  private final Map<Long, List<BigQueryError>> BIG_QUERY_ERROR_MAP = Map.of();
  private final LocalDateTime TEST_TRANSFER_DATE = LocalDateTime.now();

  private MockedConstruction<ProductsService> mockProductsServiceController;
  private MockedConstruction<BigQueryService> mockBigQueryServiceController;
  private MockedStatic<LocalDateTime> mockedStaticLocalDateTime;

  private InsertAllResponse mockInsertAllResponse;

  @Before
  public void setUp() {
    System.setProperty("feedviz.config.dir", TEST_CONFIG_DIR);

    mockInsertAllResponse = mock(InsertAllResponse.class);
    when(mockInsertAllResponse.getInsertErrors()).thenReturn(BIG_QUERY_ERROR_MAP);

    mockedStaticLocalDateTime = mockStatic(LocalDateTime.class);
    mockedStaticLocalDateTime.when(LocalDateTime::now).thenReturn(TEST_TRANSFER_DATE);

    mockProductsServiceController =
        mockConstruction(
            ProductsService.class,
            (mock, context) -> {
              when(mock.listCssProducts()).thenReturn(CSS_PRODUCT_LIST);
            });
    mockBigQueryServiceController =
        mockConstruction(
            BigQueryService.class,
            (mock, context) -> {
              when(mock.insertCssProducts(
                      TEST_DATASET_NAME,
                      TEST_DATASET_LOCATION,
                      CSS_PRODUCT_LIST,
                      TEST_TRANSFER_DATE))
                  .thenReturn(mockInsertAllResponse);
            });
  }

  @After
  public void tearDown() {
    mockedStaticLocalDateTime.close();
    mockProductsServiceController.close();
    mockBigQueryServiceController.close();
  }

  @Test
  public void testMain() {
    TransferCssProducts.main(null);
    ProductsService mockProductsService = mockProductsServiceController.constructed().get(0);
    verify(mockProductsService).listCssProducts();

    BigQueryService mockBigQueryService = mockBigQueryServiceController.constructed().get(0);
    verify(mockBigQueryService)
        .insertCssProducts(
            TEST_DATASET_NAME, TEST_DATASET_LOCATION, CSS_PRODUCT_LIST, TEST_TRANSFER_DATE);

    verify(mockInsertAllResponse).getInsertErrors();
  }
}
