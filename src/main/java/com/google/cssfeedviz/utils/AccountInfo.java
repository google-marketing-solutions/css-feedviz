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

import com.google.api.client.json.JsonParser;
import com.google.api.client.json.JsonToken;
import com.google.api.client.json.gson.GsonFactory;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;

/**
 * Wrapper for the JSON configuration file used to keep user specific details like the CSS Center
 * account ID.
 */
public class AccountInfo {
  private static final String CONFIG_DIR = "./config";
  private static final String FILE_NAME = "account-info.json";

  private BigInteger merchantId;

  private BigInteger domainId;

  private BigInteger groupId;

  private File path;

  public File getPath() {
    return path;
  }

  public void setPath(File path) {
    this.path = path;
  }

  public static AccountInfo load() throws IOException {
    return load(CONFIG_DIR, FILE_NAME);
  }

  public static AccountInfo load(String configDir, String fileName) throws IOException {
    File configPath = new File(configDir);
    if (!configPath.exists()) {
      throw new FileNotFoundException(
          "CSS API configuration directory '" + configPath.getCanonicalPath() + "' does not exist");
    }
    File configFile = new File(configPath, fileName);
    try (InputStream inputStream = new FileInputStream(configFile)) {
      AccountInfo config = new AccountInfo();
      JsonParser jParser = new GsonFactory().createJsonParser(inputStream);
      while (jParser.nextToken() != JsonToken.END_OBJECT) {
        String fieldname = jParser.getCurrentName();
        if ("merchantId".equals(fieldname)) {
          jParser.nextToken();
          config.setMerchantId(new BigInteger(jParser.getText()));
        }
        if ("domainId".equals(fieldname)) {
          jParser.nextToken();
          config.setDomainId(new BigInteger(jParser.getText()));
        }
        if ("groupId".equals(fieldname)) {
          jParser.nextToken();
          config.setGroupId(new BigInteger(jParser.getText()));
        }
      }
      jParser.close();
      config.setPath(configPath);
      return config;
    } catch (IOException e) {
      throw new IOException(
          "Could not find or read the config file at "
              + configFile.getCanonicalPath()
              + ". You can use the "
              + FILE_NAME
              + " file in the "
              + "samples root as a template.");
    }
  }

  public BigInteger getMerchantId() {
    return merchantId;
  }

  public void setMerchantId(BigInteger merchantId) {
    this.merchantId = merchantId;
  }

  public BigInteger getDomainId() {
    return domainId;
  }

  public void setDomainId(BigInteger domainId) {
    this.domainId = domainId;
  }

  public BigInteger getGroupId() {
    return groupId;
  }

  public void setGroupId(BigInteger groupId) {
    this.groupId = groupId;
  }
}
