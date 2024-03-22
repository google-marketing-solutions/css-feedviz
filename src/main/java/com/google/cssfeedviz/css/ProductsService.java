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

import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cssfeedviz.utils.AccountInfo;
import com.google.cssfeedviz.utils.Authenticator;
import com.google.shopping.css.v1.CssProduct;
import com.google.shopping.css.v1.CssProductsServiceClient;
import com.google.shopping.css.v1.CssProductsServiceClient.ListCssProductsPagedResponse;
import com.google.shopping.css.v1.CssProductsServiceSettings;
import com.google.shopping.css.v1.ListCssProductsRequest;
import java.io.IOException;

/** A class for handling CSS Products for a given Account */
public class ProductsService {

  private AccountInfo accountInfo;
  private CssProductsServiceClient cssProductsServiceClient;

  private String getParent() {
    return String.format("accounts/%s", this.accountInfo.getDomainId().toString());
  }

  public void setCssProductsServiceClient(CssProductsServiceClient cssProductsServiceClient) {
    this.cssProductsServiceClient = cssProductsServiceClient;
  }

  public Iterable<CssProduct> listCssProducts() {

    String parent = getParent();

    ListCssProductsRequest request = ListCssProductsRequest.newBuilder().setParent(parent).build();

    ListCssProductsPagedResponse response = this.cssProductsServiceClient.listCssProducts(request);
    return response.iterateAll();
  }

  public ProductsService(AccountInfo accountInfo) throws IOException {
    this.accountInfo = accountInfo;

    GoogleCredentials credential = new Authenticator().authenticate(accountInfo);

    CssProductsServiceSettings cssProductsServiceSettings =
        CssProductsServiceSettings.newBuilder()
            .setCredentialsProvider(FixedCredentialsProvider.create(credential))
            .build();

    this.cssProductsServiceClient = CssProductsServiceClient.create(cssProductsServiceSettings);
  }
}
