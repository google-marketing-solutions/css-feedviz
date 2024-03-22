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
import static org.mockito.Mockito.when;

import com.google.auth.oauth2.GoogleCredentials;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class AuthenticatorTest {

  private final String TEST_CONFIG_DIR = "./config/test";
  private final String INVALID_TEST_CONFIG_DIR = TEST_CONFIG_DIR + "/invalid";
  private final String SERVICE_ACCOUNT_FILE_PATH = TEST_CONFIG_DIR + "/service-account.json";

  @Mock private AccountInfo accountInfo;

  @InjectMocks private Authenticator authenticator;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void testAuthenticate_serviceAccount() throws IOException {
    when(accountInfo.getPath()).thenReturn(new File(TEST_CONFIG_DIR));

    GoogleCredentials expectedCredentials =
        GoogleCredentials.fromStream(new FileInputStream(SERVICE_ACCOUNT_FILE_PATH));
    GoogleCredentials credentials = authenticator.authenticate(accountInfo);

    assertEquals(expectedCredentials, credentials);
  }

  @Test
  public void testAuthenticate_serviceAccount_noPath() throws IOException {
    when(accountInfo.getPath()).thenReturn(null);

    assertThrows(IllegalArgumentException.class, () -> authenticator.authenticate(accountInfo));
  }

  @Test
  public void testAuthenticate_serviceAccount_fileNotFound() throws IOException {
    when(accountInfo.getPath()).thenReturn(new File(INVALID_TEST_CONFIG_DIR));

    assertThrows(IOException.class, () -> authenticator.authenticate(accountInfo));
  }
}
