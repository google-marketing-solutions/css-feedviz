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
package com.google.cssfeedviz.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

import java.io.IOException;
import java.math.BigInteger;
import org.junit.Test;

public class AccountInfoTest {

  private final String TEST_CONFIG_DIR = "./config/test";
  private final String INVALID_CONFIG_DIR = "./config/test/invalid";
  private final String FILE_NAME = "account-info.json";
  private final String INVALID_FILE_NAME = "invalid-account-info.json";

  private final BigInteger TEST_GROUP_ID = BigInteger.valueOf(123);
  private final BigInteger TEST_DOMAIN_ID = BigInteger.valueOf(456);
  private final BigInteger TEST_MERCHANT_ID = BigInteger.valueOf(789);

  @Test
  public void testCreate() throws IOException {
    AccountInfo accountInfo =
        AccountInfo.create(TEST_CONFIG_DIR, TEST_MERCHANT_ID, TEST_DOMAIN_ID, TEST_GROUP_ID);
    assertEquals(accountInfo.getGroupId(), TEST_GROUP_ID);
    assertEquals(accountInfo.getDomainId(), TEST_DOMAIN_ID);
    assertEquals(accountInfo.getMerchantId(), TEST_MERCHANT_ID);
  }

  @Test
  public void testCreate_withOnlyMerchantId() throws IOException {
    AccountInfo accountInfo = AccountInfo.create(TEST_CONFIG_DIR, TEST_MERCHANT_ID, null, null);
    assertEquals(accountInfo.getGroupId(), null);
    assertEquals(accountInfo.getDomainId(), null);
    assertEquals(accountInfo.getMerchantId(), TEST_MERCHANT_ID);
  }

  @Test
  public void testCreate_withOnlyDomainId() throws IOException {
    AccountInfo accountInfo = AccountInfo.create(TEST_CONFIG_DIR, null, TEST_DOMAIN_ID, null);
    assertEquals(accountInfo.getGroupId(), null);
    assertEquals(accountInfo.getDomainId(), TEST_DOMAIN_ID);
    assertEquals(accountInfo.getMerchantId(), null);
  }

  @Test
  public void testCreate_withOnlyGroupId() throws IOException {
    AccountInfo accountInfo = AccountInfo.create(TEST_CONFIG_DIR, null, null, TEST_GROUP_ID);
    assertEquals(accountInfo.getGroupId(), TEST_GROUP_ID);
    assertEquals(accountInfo.getDomainId(), null);
    assertEquals(accountInfo.getMerchantId(), null);
  }

  @Test
  public void testLoad() throws IOException {
    AccountInfo accountInfo = AccountInfo.load(TEST_CONFIG_DIR, FILE_NAME);
    assertEquals(accountInfo.getGroupId(), TEST_GROUP_ID);
    assertEquals(accountInfo.getDomainId(), TEST_DOMAIN_ID);
    assertEquals(accountInfo.getMerchantId(), TEST_MERCHANT_ID);
  }

  @Test
  public void testLoad_fileNotFound() {
    assertThrows(IOException.class, () -> AccountInfo.load(INVALID_CONFIG_DIR, FILE_NAME));
  }

  @Test
  public void testLoad_invalidFile() {
    assertThrows(IOException.class, () -> AccountInfo.load(TEST_CONFIG_DIR, INVALID_FILE_NAME));
  }
}
