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

import com.google.auth.oauth2.GoogleCredentials;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Class that contains all the authentication logic, both for service accounts and to create an
 * OAuth 2 refresh token for the CSS API.
 *
 * <p>IMPORTANT FOR OAUTH: For web app clients types, you must add {@code http://127.0.0.1} to the
 * "Authorized redirect URIs" list in your Google Cloud Console project before running this example.
 * Desktop app client types do not require the local redirect to be explicitly configured in the
 * console.
 *
 * <p>This example will start a basic server that listens for requests at {@code
 * http://127.0.0.1:PORT}, where {@code PORT} is dynamically assigned.
 */
public class Authenticator {

  public GoogleCredentials authenticate() throws IOException {
    return authenticate(AccountInfo.load());
  }

  public GoogleCredentials authenticate(AccountInfo accountInfo) throws IOException {
    if (accountInfo.getPath() == null) {
      throw new IllegalArgumentException(
          "Must update AccountInfo.java to set a configuration directory.");
    }
    File serviceAccountFile = new File(accountInfo.getPath(), "service-account.json");
    System.out.printf("Checking for service account file at: %s%n", serviceAccountFile);
    if (serviceAccountFile.exists()) {
      System.out.println("Attempting to load service account credentials");
      try (InputStream inputStream = new FileInputStream(serviceAccountFile)) {
        GoogleCredentials credential = GoogleCredentials.fromStream(inputStream);
        System.out.println("Successfully loaded service account credentials");
        return credential;
      }
    } else {
      throw new IOException(
          "Could not retrieve service account credentials from the file "
              + serviceAccountFile.getCanonicalPath());
    }
  }
}
