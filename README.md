# CSS FeedViz

## Updates

* [August 2024]: Introduced multi-threading to process the streaming of CSS Products from the [Comparison Shopping Service API](https://developers.google.com/comparison-shopping-services/api/overview) to [BigQuery](https://cloud.google.com/bigquery).

* [June 2024]: Modified the codebase to stream CSS Products into BigQuery instead of using the [insertAll method](https://cloud.google.com/java/docs/reference/google-cloud-bigquery/latest/com.google.cloud.bigquery.BigQuery#com_google_cloud_bigquery_BigQuery_insertAll_com_google_cloud_bigquery_InsertAllRequest_) due to hitting payload size issues.


## Overview

CSS FeedViz provides the means for viewing detailed attribute data on disapproved products that have been submitted to the CSS API. The provided information will improve the speed and accuracy with which CSSs will be able to respond to product disapprovals.


## Before you begin

As the CSS FeedViz dashboard uses the [Comparison Shopping Service API](https://developers.google.com/comparison-shopping-services/api/overview) and [BigQuery](https://cloud.google.com/bigquery) for data retrieval and storage, you need a [Google Cloud project](https://cloud.google.com/resource-manager/docs/creating-managing-projects) and a [service account](https://cloud.google.com/iam/docs/service-accounts-create). You will need to enable both the [CSS API](https://console.cloud.google.com/apis/library/css.googleapis.com) and [BigQuery API](https://console.cloud.google.com/apis/library/bigquery.googleapis.com) in your Google Cloud project.


## Required permissions

The service account that you chose to use for CSS FeedViz requires certain access permissions both in [Google Cloud](https://cloud.google.com/iam/docs/manage-access-service-accounts) and on [CSS Center](https://developers.google.com/comparison-shopping-services/api/guides/quickstart#permissions). 


### Google CSS Center

The email address of the service account needs to be added with _Admin_ access permissions to your CSS group or domain account in Google CSS Center.


### Google Cloud

The service account requires rights for getting, creating and updating both [datasets](https://cloud.google.com/bigquery/docs/datasets-intro) and [tables](https://cloud.google.com/bigquery/docs/tables-intro) within BigQuery. The `bigquery.dataEditor` predefined IAM role provides adequate permissions for these purposes. In Google Cloud, add the `bigquery.dataEditor` IAM role to the service account. Instructions on how to manage IAM roles for service accounts can be found at: \
https://cloud.google.com/iam/docs/manage-access-service-accounts.


## CSS Products Transfer


### Configuration Files

CSS Feedviz uses the [Comparison Shopping Services API](https://developers.google.com/comparison-shopping-services/api/guides/quickstart) to retrieve products at CSS domain level. Using this API requires a JSON private key file for the service account, which can be obtained from Google Cloud by following the instructions at: \
https://cloud.google.com/iam/docs/keys-create-delete#creating

Once you’ve obtained the JSON private key file, rename it as `service-account.json` and store it in 

```
$(HOME)/css-feedviz/config
```

In order to provide CSS FeedViz with the relevant CSS Center account information, copy the test account-info.json file from $(HOME)/css-feedviz/config/test to $(HOME)/css-feedviz/config and modify its contents as needed. The file contains a JSON object with the following fields:

| Field name | Type | Description |
| :--- | :--- | :--- |
| groupId | number | The CSS Group ID. |
| domainId | number | The CSS Domain ID to retrieve products for. |

### Transfer Execution

The CSS FeedViz codebase is managed by [Maven](https://maven.apache.org/), with the `pom.xml` configuration file located in the `css-feedviz` root directory. To compile the codebase, execute the following command from `$(HOME)/css-feedviz`.

```
mvn compile
```

Once the code has been successfully compiled, execute the following command to transfer product data from the Comparison Shopping Services API to BigQuery.

```
mvn exec:java -Dexec.mainClass="com.google.cssfeedviz.TransferCssProducts"
```

## Looker Studio Dashboard

To access the CSS FeedViz Looker Studio dashboard template, first [join the CSS FeedViz Google Group](https://groups.google.com/g/css-feedviz/). Once you’ve joined, make a copy of [this Looker Studio template](https://lookerstudio.google.com/c/u/0/reporting/94023eb5-ce2f-445a-94c5-63112318de63/page/TLFIC/preview).

Configure the Looker Studio [BigQuery Connector](https://support.google.com/looker-studio/topic/10587734) to connect to the css\_feedviz BigQuery table that has been created by the CSS Products data transfer. This can be done under the **Resource** > **Manage added data sources** menu item in Looker Studio. Further information on configuring the Looker Studio BigQuery connector can be found [here](https://support.google.com/looker-studio/answer/6370296).

Once properly configured, you should have a dashboard with two populated pages, one with a CSS level attributes and the other with Product level attributes.

## Technical Details

### TransferCssProducts System Properties

The TransferCssProducts service supports the following Java system properties to allow for greater customisation of functionality.

| System property | Default value | Description |
| :--- | :--- | :--- |
| feedviz.config.dir | `"./config"` | Path to directory containing configuration files |
| feedviz.account.info.file | `"account-info.json"` | Name of file containing CSS Center account details. |
| feedviz.dataset.name | `"css_feedviz"` | Name of the dataset that will be used/created in BigQuery. |
| feedviz.dataset.location | `"EU"` | Location of Google Cloud servers where data will be stored/processed.

### CSS Products Schema

The schema of the css\_products table in BigQuery aligns with the [CssProduct resource in CSS API](https://developers.devsite.corp.google.com/comparison-shopping-services/api/reference/rest/v1/accounts.cssProducts) as follows:

<table>
  <tr>
   <td colspan="4"><strong>Field name</strong>
   </td>
   <td><strong>Type</strong>
   </td>
   <td><strong>Description</strong>
   </td>
  </tr>
  <tr>
   <td colspan="4">transfer_date
   </td>
   <td>TIMESTAMP
   </td>
   <td>Timestamp of when transfer was initiated.
   </td>
  </tr>
  <tr>
   <td colspan="4">name
   </td>
   <td>STRING
   </td>
   <td>The name of the CSS Product. Format: "<strong>accounts/{account}/cssProducts/{css_product}</strong>"
   </td>
  </tr>
  <tr>
   <td colspan="4">raw_provided_id
   </td>
   <td>STRING
   </td>
   <td>A unique raw identifier for the product.
   </td>
  </tr>
  <tr>
   <td colspan="4">content_language
   </td>
   <td>STRING
   </td>
   <td>The two-letter <a href="http://en.wikipedia.org/wiki/ISO_639-1">ISO 639-1</a> language code for the product.
   </td>
  </tr>
  <tr>
   <td colspan="4">feed_label
   </td>
   <td>STRING
   </td>
   <td>The feed label for the product.
   </td>
  </tr>
  <tr>
   <td colspan="4">attributes
   </td>
   <td>STRUCT
   </td>
   <td>A list of product attributes.
   </td>
  </tr>
  <tr>
   <td rowspan="70">
   </td>
   <td colspan="3">low_price
   </td>
   <td>STRUCT
   </td>
   <td>Low Price of the aggregate offer.
   </td>
  </tr>
  <tr>
   <td rowspan="2">
   </td>
   <td colspan="2">amount_micros
   </td>
   <td>INT64
   </td>
   <td>The price represented as a number in micros.*
   </td>
  </tr>
  <tr>
   <td colspan="2">currency_code
   </td>
   <td>STRING
   </td>
   <td>The currency of the price using three-letter acronyms according to <a href="http://en.wikipedia.org/wiki/ISO_4217">ISO 4217</a>.
   </td>
  </tr>
  <tr>
   <td colspan="3">high_price
   </td>
   <td>STRUCT
   </td>
   <td>High Price of the aggregate offer.
   </td>
  </tr>
  <tr>
   <td rowspan="2">
   </td>
   <td colspan="2">amount_micros
   </td>
   <td>INT64
   </td>
   <td>The price represented as a number in micros.*
   </td>
  </tr>
  <tr>
   <td colspan="2">currency_code
   </td>
   <td>STRING
   </td>
   <td>The currency of the price using three-letter acronyms according to <a href="http://en.wikipedia.org/wiki/ISO_4217">ISO 4217</a>.
   </td>
  </tr>
  <tr>
   <td colspan="3">headline_offer_price
   </td>
   <td>STRUCT
   </td>
   <td>Headline Price of the aggregate offer.
   </td>
  </tr>
  <tr>
   <td rowspan="2">
   </td>
   <td colspan="2">amount_micros
   </td>
   <td>INT64
   </td>
   <td>The price represented as a number in micros.*
   </td>
  </tr>
  <tr>
   <td colspan="2">currency_code
   </td>
   <td>STRING
   </td>
   <td>The currency of the price using three-letter acronyms according to <a href="http://en.wikipedia.org/wiki/ISO_4217">ISO 4217</a>.
   </td>
  </tr>
  <tr>
   <td colspan="3">headline_offer_shipping_price
   </td>
   <td>STRUCT
   </td>
   <td>The shipping price of the headline offer for the product
   </td>
  </tr>
  <tr>
   <td rowspan="2">
   </td>
   <td colspan="2">amount_micros
   </td>
   <td>INT64
   </td>
   <td>The price represented as a number in micros.*
   </td>
  </tr>
  <tr>
   <td colspan="2">currency_code
   </td>
   <td>STRING
   </td>
   <td>The currency of the price using three-letter acronyms according to <a href="http://en.wikipedia.org/wiki/ISO_4217">ISO 4217</a>.
   </td>
  </tr>
  <tr>
   <td colspan="3">additional_image_links
   </td>
   <td>STRING, REPEATED
   </td>
   <td>Additional URL of images of the item.
   </td>
  </tr>
  <tr>
   <td colspan="3">product_types
   </td>
   <td>STRING, REPEATED
   </td>
   <td>Categories of the item (formatted as in <a href="https://support.google.com/merchants/answer/6324406">products data specification</a>).
   </td>
  </tr>
  <tr>
   <td colspan="3">size_types
   </td>
   <td>STRING, REPEATED
   </td>
   <td>The cut of the item. It can be used to represent combined size types for apparel items. Maximum two of size types can be provided (see <a href="https://support.google.com/merchants/answer/6324497">size types</a>).
   </td>
  </tr>
  <tr>
   <td colspan="3">product_details
   </td>
   <td>STRUCT, REPEATED
   </td>
   <td>Technical specification or additional product details.
   </td>
  </tr>
  <tr>
   <td rowspan="3">
   </td>
   <td colspan="2">section_name
   </td>
   <td>STRING
   </td>
   <td>The section header used to group a set of product details.
   </td>
  </tr>
  <tr>
   <td colspan="2">attribute_name
   </td>
   <td>STRING
   </td>
   <td>The name of the product detail.
   </td>
  </tr>
  <tr>
   <td colspan="2">attribute_value
   </td>
   <td>STRING
   </td>
   <td>The value of the product detail.
   </td>
  </tr>
  <tr>
   <td colspan="3">product_weight
   </td>
   <td>STRUCT
   </td>
   <td>The weight of the product in the units provided.†
   </td>
  </tr>
  <tr>
   <td rowspan="2">
   </td>
   <td colspan="2">value
   </td>
   <td>FLOAT64
   </td>
   <td>The weight represented as a number.‡
   </td>
  </tr>
  <tr>
   <td colspan="2">unit
   </td>
   <td>STRING
   </td>
   <td>The weight unit. Acceptable values are:  "<strong>g</strong>",  "<strong>kg</strong>",  "<strong>oz</strong>", "<strong>lb</strong>".
   </td>
  </tr>
  <tr>
   <td colspan="3">product_length
   </td>
   <td>STRUCT
   </td>
   <td>The length of the product in the units provided.§
   </td>
  </tr>
  <tr>
   <td rowspan="2">
   </td>
   <td colspan="2">value
   </td>
   <td>FLOAT64
   </td>
   <td>The dimension value represented as a number.‡
   </td>
  </tr>
  <tr>
   <td colspan="2">unit
   </td>
   <td>STRING
   </td>
   <td>The dimension units. Acceptable values are: "<strong>in</strong>", "<strong>cm</strong>".
   </td>
  </tr>
  <tr>
   <td colspan="3">product_width
   </td>
   <td>STRUCT
   </td>
   <td>The width of the product in the units provided.§
   </td>
  </tr>
  <tr>
   <td rowspan="2">
   </td>
   <td colspan="2">value
   </td>
   <td>FLOAT64
   </td>
   <td>The dimension value represented as a number.‡
   </td>
  </tr>
  <tr>
   <td colspan="2">unit
   </td>
   <td>STRING
   </td>
   <td>The dimension units. Acceptable values are: "<strong>in</strong>", "<strong>cm</strong>"
   </td>
  </tr>
  <tr>
   <td colspan="3">product_height
   </td>
   <td>STRUCT
   </td>
   <td>The height of the product in the units provided.§
   </td>
  </tr>
  <tr>
   <td rowspan="2">
   </td>
   <td colspan="2">value
   </td>
   <td>FLOAT64
   </td>
   <td>The dimension value represented as a number.‡
   </td>
  </tr>
  <tr>
   <td colspan="2">unit
   </td>
   <td>STRING
   </td>
   <td>The dimension units. Acceptable values are: "<strong>in</strong>", "<strong>cm</strong>"
   </td>
  </tr>
  <tr>
   <td colspan="3">product_highlights
   </td>
   <td>STRING, REPEATED
   </td>
   <td>Bullet points describing the most relevant highlights of a product.
   </td>
  </tr>
  <tr>
   <td colspan="3">certifications
   </td>
   <td>STRUCT, REPEATED
   </td>
   <td>A list of certificates claimed by the CSS for the given product.
   </td>
  </tr>
  <tr>
   <td rowspan="3">
   </td>
   <td colspan="2">name
   </td>
   <td>STRING
   </td>
   <td>Name of the certification.
   </td>
  </tr>
  <tr>
   <td colspan="2">authority
   </td>
   <td>STRING
   </td>
   <td>Name of the certification body.
   </td>
  </tr>
  <tr>
   <td colspan="2">code
   </td>
   <td>STRING
   </td>
   <td>A unique code to identify the certification.
   </td>
  </tr>
  <tr>
   <td colspan="3">expiration_date
   </td>
   <td>TIMESTAMP
   </td>
   <td>Date on which the item should expire, as specified upon insertion, in <a href="https://support.google.com/merchants/answer/7055760">ISO 8601</a> format.¶
   </td>
  </tr>
  <tr>
   <td colspan="3">included_destinations
   </td>
   <td>STRING, REPEATED
   </td>
   <td>The list of destinations to include for this target (corresponds to checked check boxes in Merchant Center). Default destinations are always included unless provided in <strong>excluded_destinations</strong>.
   </td>
  </tr>
  <tr>
   <td colspan="3">excluded_destinations
   </td>
   <td>STRING, REPEATED
   </td>
   <td>The list of destinations to exclude for this target (corresponds to unchecked check boxes in Merchant Center).
   </td>
  </tr>
  <tr>
   <td colspan="3">cpp_link
   </td>
   <td>STRING
   </td>
   <td>URL directly linking to the Product Detail Page of the CSS.
   </td>
  </tr>
  <tr>
   <td colspan="3">cpp_mobile_link
   </td>
   <td>STRING
   </td>
   <td>URL for the mobile-optimized version of the Product Detail Page of the CSS.
   </td>
  </tr>
  <tr>
   <td colspan="3">cpp_ads_redirect
   </td>
   <td>STRING
   </td>
   <td>Allows advertisers to override the item URL when the product is shown within the context of Product Ads.
   </td>
  </tr>
  <tr>
   <td colspan="3">number_of_offers
   </td>
   <td>INT64
   </td>
   <td>The number of aggregate offers.
   </td>
  </tr>
  <tr>
   <td colspan="3">headline_offer_condition
   </td>
   <td>STRING
   </td>
   <td>Condition of the headline offer.
   </td>
  </tr>
  <tr>
   <td colspan="3">headline_offer_link
   </td>
   <td>STRING
   </td>
   <td>Link to the headline offer.
   </td>
  </tr>
  <tr>
   <td colspan="3">headline_offer_mobile_link
   </td>
   <td>STRING
   </td>
   <td>Mobile Link to the headline offer.
   </td>
  </tr>
  <tr>
   <td colspan="3">title
   </td>
   <td>STRING
   </td>
   <td>Title of the item.
   </td>
  </tr>
  <tr>
   <td colspan="3">image_link
   </td>
   <td>STRING
   </td>
   <td>URL of an image of the item.
   </td>
  </tr>
  <tr>
   <td colspan="3">description
   </td>
   <td>STRING
   </td>
   <td>Description of the item.
   </td>
  </tr>
  <tr>
   <td colspan="3">brand
   </td>
   <td>STRING
   </td>
   <td>Brand of the item.
   </td>
  </tr>
  <tr>
   <td colspan="3">mpn
   </td>
   <td>STRING
   </td>
   <td>Manufacturer Part Number (<a href="https://support.google.com/merchants/answer/188494#mpn">MPN</a>) of the item.
   </td>
  </tr>
  <tr>
   <td colspan="3">gtin
   </td>
   <td>STRING
   </td>
   <td>Global Trade Item Number (<a href="https://support.google.com/merchants/answer/188494#gtin">GTIN</a>) of the item.
   </td>
  </tr>
  <tr>
   <td colspan="3">google_product_category
   </td>
   <td>STRING
   </td>
   <td>Google's category of the item (see <a href="https://support.google.com/merchants/answer/1705911">Google product taxonomy</a>).#
   </td>
  </tr>
  <tr>
   <td colspan="3">adult
   </td>
   <td>BOOL
   </td>
   <td>Set to true if the item is targeted towards adults.
   </td>
  </tr>
  <tr>
   <td colspan="3">multipack
   </td>
   <td>INT64
   </td>
   <td>The number of identical products in a merchant-defined multipack.
   </td>
  </tr>
  <tr>
   <td colspan="3">is_bundle
   </td>
   <td>BOOL
   </td>
   <td>Whether the item is a merchant-defined bundle. A bundle is a custom grouping of different products sold by a merchant for a single price.
   </td>
  </tr>
  <tr>
   <td colspan="3">age_group
   </td>
   <td>STRING
   </td>
   <td>Target age group of the item.
   </td>
  </tr>
  <tr>
   <td colspan="3">color
   </td>
   <td>STRING
   </td>
   <td>Color of the item.
   </td>
  </tr>
  <tr>
   <td colspan="3">gender
   </td>
   <td>STRING
   </td>
   <td>Target gender of the item.
   </td>
  </tr>
  <tr>
   <td colspan="3">material
   </td>
   <td>STRING
   </td>
   <td>The material of which the item is made.
   </td>
  </tr>
  <tr>
   <td colspan="3">pattern
   </td>
   <td>STRING
   </td>
   <td>The item's pattern (e.g. polka dots).
   </td>
  </tr>
  <tr>
   <td colspan="3">size
   </td>
   <td>STRING
   </td>
   <td>Size of the item.
   </td>
  </tr>
  <tr>
   <td colspan="3">size_system
   </td>
   <td>STRING
   </td>
   <td>System in which the size is specified. Recommended for apparel items.
   </td>
  </tr>
  <tr>
   <td colspan="3">item_group_id
   </td>
   <td>STRING
   </td>
   <td>Shared identifier for all variants of the same product.
   </td>
  </tr>
  <tr>
   <td colspan="3">pause
   </td>
   <td>STRING
   </td>
   <td>Publication of this item will be temporarily paused.
   </td>
  </tr>
  <tr>
   <td colspan="3">custom_label_0
   </td>
   <td>STRING
   </td>
   <td>Custom label 0 for custom grouping of items in a Shopping campaign.
   </td>
  </tr>
  <tr>
   <td colspan="3">custom_label_1
   </td>
   <td>STRING
   </td>
   <td>Custom label 1 for custom grouping of items in a Shopping campaign.
   </td>
  </tr>
  <tr>
   <td colspan="3">custom_label_2
   </td>
   <td>STRING
   </td>
   <td>Custom label 2 for custom grouping of items in a Shopping campaign.
   </td>
  </tr>
  <tr>
   <td colspan="3">custom_label_3
   </td>
   <td>STRING
   </td>
   <td>Custom label 3 for custom grouping of items in a Shopping campaign.
   </td>
  </tr>
  <tr>
   <td colspan="3">custom_label_4
   </td>
   <td>STRING
   </td>
   <td>Custom label 4 for custom grouping of items in a Shopping campaign.
   </td>
  </tr>
  <tr>
   <td colspan="4">css_product_status
   </td>
   <td>STRUCT
   </td>
   <td>The status of a product, data validation issues, that is, information about a product computed asynchronously.
   </td>
  </tr>
  <tr>
   <td rowspan="18">
   </td>
   <td colspan="3">destination_statuses
   </td>
   <td>STRUCT, REPEATED
   </td>
   <td>The intended destinations for the product.
   </td>
  </tr>
  <tr>
   <td rowspan="4">
   </td>
   <td colspan="2">destination
   </td>
   <td>STRING
   </td>
   <td>The name of the destination
   </td>
  </tr>
  <tr>
   <td colspan="2">approved_countries
   </td>
   <td>STRING, REPEATED
   </td>
   <td>List of country codes (<a href="https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2">ISO 3166-1 alpha-2</a>) where the aggregate offer is approved.
   </td>
  </tr>
  <tr>
   <td colspan="2">pending_countries
   </td>
   <td>STRING, REPEATED
   </td>
   <td>List of country codes (<a href="https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2">ISO 3166-1 alpha-2</a>) where the aggregate offer is pending approval.
   </td>
  </tr>
  <tr>
   <td colspan="2">disapproved_countries
   </td>
   <td>STRING, REPEATED
   </td>
   <td>List of country codes (<a href="https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2">ISO 3166-1 alpha-2</a>) where the aggregate offer is disapproved.
   </td>
  </tr>
  <tr>
   <td colspan="3">item_level_issues
   </td>
   <td>STRUCT, REPEATED
   </td>
   <td>A list of all issues associated with the product.
   </td>
  </tr>
  <tr>
   <td rowspan="9">
   </td>
   <td colspan="2">code
   </td>
   <td>STRING
   </td>
   <td>The error code of the issue.
   </td>
  </tr>
  <tr>
   <td colspan="2">servability
   </td>
   <td>STRING
   </td>
   <td>How this issue affects the serving of the aggregate offer.
   </td>
  </tr>
  <tr>
   <td colspan="2">resolution
   </td>
   <td>STRING
   </td>
   <td>Whether the issue can be resolved by the merchant.
   </td>
  </tr>
  <tr>
   <td colspan="2">attribute
   </td>
   <td>STRING
   </td>
   <td>The attribute's name, if the issue is caused by a single attribute.
   </td>
  </tr>
  <tr>
   <td colspan="2">destination
   </td>
   <td>STRING
   </td>
   <td>The destination the issue applies to.
   </td>
  </tr>
  <tr>
   <td colspan="2">description
   </td>
   <td>STRING
   </td>
   <td>A short issue description in English.
   </td>
  </tr>
  <tr>
   <td colspan="2">detail
   </td>
   <td>STRING
   </td>
   <td>A detailed issue description in English.
   </td>
  </tr>
  <tr>
   <td colspan="2">documentation
   </td>
   <td>STRING
   </td>
   <td>The URL of a web page to help with resolving this issue.
   </td>
  </tr>
  <tr>
   <td colspan="2">applicable_countries
   </td>
   <td>STRING, REPEATED
   </td>
   <td>List of country codes (<a href="https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2">ISO 3166-1 alpha-2</a>) where the issue applies to the aggregate offer.
   </td>
  </tr>
  <tr>
   <td colspan="3">creation_date
   </td>
   <td>TIMESTAMP
   </td>
   <td>Date on which the item has been created, in <a href="http://en.wikipedia.org/wiki/ISO_8601">ISO 8601</a> format.
   </td>
  </tr>
  <tr>
   <td colspan="3">last_update_date
   </td>
   <td>TIMESTAMP
   </td>
   <td>Date on which the item has been last updated, in <a href="http://en.wikipedia.org/wiki/ISO_8601">ISO 8601</a> format.
   </td>
  </tr>
  <tr>
   <td colspan="3">google_expiration_date
   </td>
   <td>TIMESTAMP
   </td>
   <td>Date on which the item expires in Google Shopping, in <a href="http://en.wikipedia.org/wiki/ISO_8601">ISO 8601</a> format.
   </td>
  </tr>
</table>


\* 1 million micros is an equivalent to one's currency standard unit, for example, 1 USD = 1000000 micros.

† The value must be between 0 (exclusive) and 2000 (inclusive).

‡ The value can have a maximum precision of four decimal places.

§ The value must be between 0 (exclusive) and 3000 (inclusive).

¶ The actual expiration date is exposed in **css\_product\_status** as **google\_expiration\_date** and might be earlier if expiration\_date is too far in the future. Note: It may take 2+ days from the expiration date for the item to actually get deleted.

\# When querying products, this field will contain the user provided value. There is currently no way to get back the auto assigned google product categories through the API.
