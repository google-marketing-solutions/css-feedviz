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

package com.google.cssfeedviz.css;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

import com.google.cssfeedviz.utils.AccountInfo;
import com.google.shopping.css.v1.CssProduct;
import com.google.shopping.css.v1.CssProductsServiceClient;
import com.google.shopping.css.v1.CssProductsServiceClient.ListCssProductsPagedResponse;
import com.google.shopping.css.v1.ListCssProductsRequest;
import java.io.IOException;
import java.math.BigInteger;
import java.util.List;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class ProductsServiceTest {
  private final String PRODUCT_NAME = "Test Product Name";
  private final CssProduct CSS_PRODUCT = CssProduct.newBuilder().setName(PRODUCT_NAME).build();
  private final List<CssProduct> CSS_PRODUCT_LIST = List.of(CSS_PRODUCT);
  private final String TEST_CONFIG_DIR = "./config/test";
  private final String ACCOUNT_INFO_FILE_NAME = "account-info.json";
  private final BigInteger TEST_DOMAIN_ID = BigInteger.valueOf(456);

  @Mock private CssProductsServiceClient cssProductsServiceClient;

  @Mock private ListCssProductsPagedResponse listCssProductsPagedResponse;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void listCssProducts_returnsCssProducts() throws IOException {

    AccountInfo accountInfo = AccountInfo.load(TEST_CONFIG_DIR, ACCOUNT_INFO_FILE_NAME);

    String parent = String.format("accounts/%s", TEST_DOMAIN_ID);
    ListCssProductsRequest listCssProductsRequest =
        ListCssProductsRequest.newBuilder().setParent(parent).build();

    ProductsService productsService = new ProductsService(accountInfo);
    productsService.setCssProductsServiceClient(cssProductsServiceClient);

    when(cssProductsServiceClient.listCssProducts(listCssProductsRequest))
        .thenReturn(listCssProductsPagedResponse);
    when(listCssProductsPagedResponse.iterateAll()).thenReturn(CSS_PRODUCT_LIST);

    List<CssProduct> cssProductList = (List<CssProduct>) productsService.listCssProducts();
    assertEquals(CSS_PRODUCT_LIST.size(), cssProductList.size());
    assertTrue(cssProductList.contains(CSS_PRODUCT));
  }
}
