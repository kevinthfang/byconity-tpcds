DROP DATABASE IF EXISTS tpcds;
CREATE DATABASE tpcds;
USE tpcds;

CREATE TABLE IF NOT EXISTS customer_address
(
    `ca_address_sk`            LARGEINT NULL COMMENT "",
    `ca_address_id`            VARCHAR(16) NULL COMMENT "",
    `ca_street_number`         VARCHAR(4) NULL COMMENT "",
    `ca_street_name`           VARCHAR(21) NULL COMMENT "",
    `ca_street_type`           VARCHAR(9) NULL COMMENT "",
    `ca_suite_number`          VARCHAR(9) NULL COMMENT "",
    `ca_city`                  VARCHAR(20) NULL COMMENT "",
    `ca_county`                VARCHAR(28) NULL COMMENT "",
    `ca_state`                 VARCHAR(2) NULL COMMENT "",
    `ca_zip`                   VARCHAR(5) NULL COMMENT "",
    `ca_country`               VARCHAR(13) NULL COMMENT "",
    `ca_gmt_offset`            FLOAT NULL COMMENT "",
    `ca_location_type`         VARCHAR(13) NULL COMMENT ""
) ENGINE=OLAP 
DUPLICATE KEY(`ca_address_sk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`ca_address_sk`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "tpcdsa",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE IF NOT EXISTS customer_demographics
(
    `cd_demo_sk`               LARGEINT NULL COMMENT "",
    `cd_gender`                VARCHAR(1) NULL COMMENT "",
    `cd_marital_status`        VARCHAR(1) NULL COMMENT "",
    `cd_education_status`      VARCHAR(15) NULL COMMENT "",
    `cd_purchase_estimate`     SMALLINT NULL COMMENT "",
    `cd_credit_rating`         VARCHAR(9) NULL COMMENT "",
    `cd_dep_count`             TINYINT NULL COMMENT "",
    `cd_dep_employed_count`    TINYINT NULL COMMENT "",
    `cd_dep_college_count`     TINYINT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`cd_demo_sk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`cd_demo_sk`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "tpcdsb",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE IF NOT EXISTS date_dim
(
    `d_date_sk`                LARGEINT NULL COMMENT "",
    `d_date_id`                VARCHAR(16) NULL COMMENT "",
    `d_date`                   DATE NULL COMMENT "",
    `d_month_seq`              SMALLINT NULL COMMENT "",
    `d_week_seq`               SMALLINT NULL COMMENT "",
    `d_quarter_seq`            SMALLINT NULL COMMENT "",
    `d_year`                   SMALLINT NULL COMMENT "",
    `d_dow`                    TINYINT NULL COMMENT "",
    `d_moy`                    TINYINT NULL COMMENT "",
    `d_dom`                    TINYINT NULL COMMENT "",
    `d_qoy`                    TINYINT NULL COMMENT "",
    `d_fy_year`                SMALLINT NULL COMMENT "",
    `d_fy_quarter_seq`         SMALLINT NULL COMMENT "",
    `d_fy_week_seq`            SMALLINT NULL COMMENT "",
    `d_day_name`               VARCHAR(9) NULL COMMENT "",
    `d_quarter_name`           VARCHAR(6) NULL COMMENT "",
    `d_holiday`                VARCHAR(1) NULL COMMENT "",
    `d_weekend`                VARCHAR(1) NULL COMMENT "",
    `d_following_holiday`      VARCHAR(1) NULL COMMENT "",
    `d_first_dom`              LARGEINT NULL COMMENT "",
    `d_last_dom`               LARGEINT NULL COMMENT "",
    `d_same_day_ly`            LARGEINT NULL COMMENT "",
    `d_same_day_lq`            LARGEINT NULL COMMENT "",
    `d_current_day`            VARCHAR(1) NULL COMMENT "",
    `d_current_week`           VARCHAR(1) NULL COMMENT "",
    `d_current_month`          VARCHAR(1) NULL COMMENT "",
    `d_current_quarter`        VARCHAR(1) NULL COMMENT "",
    `d_current_year`           VARCHAR(1) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`d_date_sk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`d_date_sk`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "tpcdsc",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE IF NOT EXISTS warehouse
(
    w_warehouse_sk           TINYINT NULL COMMENT "",
    w_warehouse_id           VARCHAR(16) NULL COMMENT "",
    w_warehouse_name         VARCHAR(20) NULL COMMENT "",
    w_warehouse_sq_ft        LARGEINT NULL COMMENT "",
    w_street_number          VARCHAR(3) NULL COMMENT "",
    w_street_name            VARCHAR(15) NULL COMMENT "",
    w_street_type            VARCHAR(7) NULL COMMENT "",
    w_suite_number           VARCHAR(9) NULL COMMENT "",
    w_city                   VARCHAR(15) NULL COMMENT "",
    w_county                 VARCHAR(17) NULL COMMENT "",
    w_state                  VARCHAR(2) NULL COMMENT "",
    w_zip                    VARCHAR(5) NULL COMMENT "",
    w_country                VARCHAR(13) NULL COMMENT "",
    w_gmt_offset             FLOAT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`w_warehouse_sk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`w_warehouse_sk`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "tpcdsd",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE IF NOT EXISTS ship_mode
(
    sm_ship_mode_sk          TINYINT NULL COMMENT "",
    sm_ship_mode_id          VARCHAR(16) NULL COMMENT "",
    sm_type                  VARCHAR(9) NULL COMMENT "",
    sm_code                  VARCHAR(7) NULL COMMENT "",
    sm_carrier               VARCHAR(14) NULL COMMENT "",
    sm_contract              VARCHAR(20) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`sm_ship_mode_sk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`sm_ship_mode_sk`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "tpcdse",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE IF NOT EXISTS time_dim
(
    t_time_sk                LARGEINT NULL COMMENT "",
    t_time_id                VARCHAR(16) NULL COMMENT "",
    t_time                   LARGEINT NULL COMMENT "",
    t_hour                   TINYINT NULL COMMENT "",
    t_minute                 TINYINT NULL COMMENT "",
    t_second                 TINYINT NULL COMMENT "",
    t_am_pm                  VARCHAR(2) NULL COMMENT "",
    t_shift                  VARCHAR(6) NULL COMMENT "",
    t_sub_shift              VARCHAR(9) NULL COMMENT "",
    t_meal_time              VARCHAR(9) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`t_time_sk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`t_time_sk`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "tpcdsf",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE IF NOT EXISTS reason
(
    r_reason_sk              TINYINT NULL COMMENT "",
    r_reason_id              VARCHAR(16) NULL COMMENT "",
    r_reason_desc            VARCHAR(43) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`r_reason_sk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`r_reason_sk`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "tpcdsg",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE IF NOT EXISTS income_band
(
    ib_income_band_sk         TINYINT NULL COMMENT "",
    ib_lower_bound            LARGEINT NULL COMMENT "",
    ib_upper_bound            LARGEINT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`ib_income_band_sk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`ib_income_band_sk`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "tpcdsh",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE IF NOT EXISTS item
(
    i_item_sk                LARGEINT NULL COMMENT "",
    i_item_id                VARCHAR(16) NULL COMMENT "",
    i_rec_start_date         DATE NULL COMMENT "",
    i_rec_end_date           DATE NULL COMMENT "",
    i_item_desc              VARCHAR(200) NULL COMMENT "",
    i_current_price          DOUBLE NULL COMMENT "",
    i_wholesale_cost         FLOAT NULL COMMENT "",
    i_brand_id               BIGINT NULL COMMENT "",
    i_brand                  VARCHAR(22) NULL COMMENT "",
    i_class_id               TINYINT NULL COMMENT "",
    i_class                  VARCHAR(15) NULL COMMENT "",
    i_category_id            TINYINT NULL COMMENT "",
    i_category               VARCHAR(11) NULL COMMENT "",
    i_manufact_id            SMALLINT NULL COMMENT "",
    i_manufact               VARCHAR(15) NULL COMMENT "",
    i_size                   VARCHAR(11) NULL COMMENT "",
    i_formulation            VARCHAR(20) NULL COMMENT "",
    i_color                  VARCHAR(10) NULL COMMENT "",
    i_units                  VARCHAR(7) NULL COMMENT "",
    i_container              VARCHAR(7) NULL COMMENT "",
    i_manager_id             TINYINT NULL COMMENT "",
    i_product_name           VARCHAR(30) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`i_item_sk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`i_item_sk`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "tpcdsi",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE IF NOT EXISTS store
(
    s_store_sk               SMALLINT NULL COMMENT "",
    s_store_id               VARCHAR(17) NULL COMMENT "",
    s_rec_start_date         DATE NULL COMMENT "",
    s_rec_end_date           DATE NULL COMMENT "",
    s_closed_date_sk         BIGINT NULL COMMENT "",
    s_store_name             VARCHAR(5) NULL COMMENT "",
    s_number_employees       SMALLINT NULL COMMENT "",
    s_floor_space            LARGEINT NULL COMMENT "",
    s_hours                  VARCHAR(8) NULL COMMENT "",
    s_manager                VARCHAR(20) NULL COMMENT "",
    s_market_id              TINYINT NULL COMMENT "",
    s_geography_class        VARCHAR(7) NULL COMMENT "",
    s_market_desc            VARCHAR(100) NULL COMMENT "",
    s_market_manager         VARCHAR(20) NULL COMMENT "",
    s_division_id            TINYINT NULL COMMENT "",
    s_division_name          VARCHAR(7) NULL COMMENT "",
    s_company_id             TINYINT NULL COMMENT "",
    s_company_name           VARCHAR(7) NULL COMMENT "",
    s_street_number          VARCHAR(4) NULL COMMENT "",
    s_street_name            VARCHAR(17) NULL COMMENT "",
    s_street_type            VARCHAR(9) NULL COMMENT "",
    s_suite_number           VARCHAR(9) NULL COMMENT "",
    s_city                   VARCHAR(15) NULL COMMENT "",
    s_county                 VARCHAR(22) NULL COMMENT "",
    s_state                  VARCHAR(2) NULL COMMENT "",
    s_zip                    VARCHAR(5) NULL COMMENT "",
    s_country                VARCHAR(13) NULL COMMENT "",
    s_gmt_offset             FLOAT NULL COMMENT "",
    s_tax_precentage         FLOAT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`s_store_sk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`s_store_sk`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "tpcdsj",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE IF NOT EXISTS call_center
(
    cc_call_center_sk        TINYINT NULL COMMENT "",
    cc_call_center_id        VARCHAR(16) NULL COMMENT "",
    cc_rec_start_date        DATE NULL COMMENT "",
    cc_rec_end_date          DATE NULL COMMENT "",
    cc_closed_date_sk        LARGEINT NULL COMMENT "",
    cc_open_date_sk          LARGEINT NULL COMMENT "",
    cc_name                  VARCHAR(19) NULL COMMENT "",
    cc_class                 VARCHAR(6) NULL COMMENT "",
    cc_employees             LARGEINT NULL COMMENT "",
    cc_sq_ft                 BIGINT NULL COMMENT "",
    cc_hours                 VARCHAR(8) NULL COMMENT "",
    cc_manager               VARCHAR(17) NULL COMMENT "",
    cc_mkt_id                TINYINT NULL COMMENT "",
    cc_mkt_class             VARCHAR(50) NULL COMMENT "",
    cc_mkt_desc              VARCHAR(95) NULL COMMENT "",
    cc_market_manager        VARCHAR(17) NULL COMMENT "",
    cc_division              TINYINT NULL COMMENT "",
    cc_division_name         VARCHAR(5) NULL COMMENT "",
    cc_company               TINYINT NULL COMMENT "",
    cc_company_name          VARCHAR(5) NULL COMMENT "",
    cc_street_number         VARCHAR(3) NULL COMMENT "",
    cc_street_name           VARCHAR(25) NULL COMMENT "",
    cc_street_type           VARCHAR(9) NULL COMMENT "",
    cc_suite_number          VARCHAR(9) NULL COMMENT "",
    cc_city                  VARCHAR(25) NULL COMMENT "",
    cc_county                VARCHAR(25) NULL COMMENT "",
    cc_state                 VARCHAR(2) NULL COMMENT "",
    cc_zip                   VARCHAR(5) NULL COMMENT "",
    cc_country               VARCHAR(25) NULL COMMENT "",
    cc_gmt_offset            FLOAT NULL COMMENT "",
    cc_tax_percentage        FLOAT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`cc_call_center_sk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`cc_call_center_sk`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "tpcdsk",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE IF NOT EXISTS customer
(
    c_customer_sk            VARCHAR(256) NULL COMMENT "",
    c_customer_id            VARCHAR(256) NULL COMMENT "",
    c_current_cdemo_sk       VARCHAR(256) NULL COMMENT "",
    c_current_hdemo_sk       VARCHAR(256) NULL COMMENT "",
    c_current_addr_sk        VARCHAR(256) NULL COMMENT "",
    c_first_shipto_date_sk   VARCHAR(256) NULL COMMENT "",
    c_first_sales_date_sk    VARCHAR(256) NULL COMMENT "",
    c_salutation             VARCHAR(256) NULL COMMENT "",
    c_first_name             VARCHAR(256) NULL COMMENT "",
    c_last_name              VARCHAR(256) NULL COMMENT "",
    c_preferred_cust_flag    VARCHAR(256) NULL COMMENT "",
    c_birth_day              VARCHAR(256) NULL COMMENT "",
    c_birth_month            VARCHAR(256) NULL COMMENT "",
    c_birth_year             VARCHAR(256) NULL COMMENT "",
    c_birth_country          VARCHAR(256) NULL COMMENT "",
    c_login                  VARCHAR(256) NULL COMMENT "",
    c_email_address          VARCHAR(256) NULL COMMENT "",
    c_last_review_date       VARCHAR(256) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`c_customer_sk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`c_customer_sk`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "tpcdsl",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE IF NOT EXISTS web_site
(
    web_site_sk              TINYINT NULL COMMENT "",
    web_site_id              VARCHAR(16) NULL COMMENT "",
    web_rec_start_date       DATE NULL COMMENT "",
    web_rec_end_date         DATE NULL COMMENT "",
    web_name                 VARCHAR(50) NULL COMMENT "",
    web_open_date_sk         LARGEINT NULL COMMENT "",
    web_close_date_sk        LARGEINT NULL COMMENT "",
    web_class                VARCHAR(50) NULL COMMENT "",
    web_manager              VARCHAR(40) NULL COMMENT "",
    web_mkt_id               TINYINT NULL COMMENT "",
    web_mkt_class            VARCHAR(50) NULL COMMENT "",
    web_mkt_desc             VARCHAR(100) NULL COMMENT "",
    web_market_manager       VARCHAR(40) NULL COMMENT "",
    web_company_id           TINYINT NULL COMMENT "",
    web_company_name         VARCHAR(50) NULL COMMENT "",
    web_street_number        VARCHAR(10) NULL COMMENT "",
    web_street_name          VARCHAR(60) NULL COMMENT "",
    web_street_type          VARCHAR(15) NULL COMMENT "",
    web_suite_number         VARCHAR(10) NULL COMMENT "",
    web_city                 VARCHAR(60) NULL COMMENT "",
    web_county               VARCHAR(30) NULL COMMENT "",
    web_state                VARCHAR(2) NULL COMMENT "",
    web_zip                  VARCHAR(10) NULL COMMENT "",
    web_country              VARCHAR(20) NULL COMMENT "",
    web_gmt_offset           FLOAT NULL COMMENT "",
    web_tax_percentage       FLOAT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`web_site_sk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`web_site_sk`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "tpcdsm",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE IF NOT EXISTS store_returns
(
    sr_returned_date_sk       LARGEINT NULL COMMENT "",
    sr_return_time_sk         LARGEINT NULL COMMENT "",
    sr_item_sk                LARGEINT NULL COMMENT "",
    sr_customer_sk            LARGEINT NULL COMMENT "",
    sr_cdemo_sk               LARGEINT NULL COMMENT "",
    sr_hdemo_sk               SMALLINT NULL COMMENT "",
    sr_addr_sk                LARGEINT NULL COMMENT "",
    sr_store_sk               SMALLINT NULL COMMENT "",
    sr_reason_sk              TINYINT NULL COMMENT "",
    sr_ticket_number          LARGEINT NULL COMMENT "",
    sr_return_quantity        TINYINT NULL COMMENT "",
    sr_return_amt             FLOAT NULL COMMENT "",
    sr_return_tax             FLOAT NULL COMMENT "",
    sr_return_amt_inc_tax     FLOAT NULL COMMENT "",
    sr_fee                    FLOAT NULL COMMENT "",
    sr_return_ship_cost       FLOAT NULL COMMENT "",
    sr_refunded_cash          FLOAT NULL COMMENT "",
    sr_reversed_charge        FLOAT NULL COMMENT "",
    sr_store_credit           FLOAT NULL COMMENT "",
    sr_net_loss               FLOAT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`sr_returned_date_sk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`sr_returned_date_sk`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "tpcdsn",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE IF NOT EXISTS household_demographics
(
    hd_demo_sk                SMALLINT NULL COMMENT "",
    hd_income_band_sk         TINYINT NULL COMMENT "",
    hd_buy_potential          VARCHAR(10) NULL COMMENT "",
    hd_dep_count              TINYINT NULL COMMENT "",
    hd_vehicle_count          TINYINT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`hd_demo_sk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`hd_demo_sk`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "tpcdso",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE IF NOT EXISTS web_page
(
    wp_web_page_sk           SMALLINT NULL COMMENT "",
    wp_web_page_id           VARCHAR(16) NULL COMMENT "",
    wp_rec_start_date        DATE NULL COMMENT "",
    wp_rec_end_date          DATE NULL COMMENT "",
    wp_creation_date_sk      LARGEINT NULL COMMENT "",
    wp_access_date_sk        LARGEINT NULL COMMENT "",
    wp_autogen_flag          VARCHAR(1) NULL COMMENT "",
    wp_customer_sk           LARGEINT NULL COMMENT "",
    wp_url                   VARCHAR(18) NULL COMMENT "",
    wp_type                  VARCHAR(9) NULL COMMENT "",
    wp_char_count            SMALLINT NULL COMMENT "",
    wp_link_count            TINYINT NULL COMMENT "",
    wp_image_count           TINYINT NULL COMMENT "",
    wp_max_ad_count          TINYINT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`wp_web_page_sk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`wp_web_page_sk`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "tpcdsp",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE IF NOT EXISTS promotion
(
    p_promo_sk               SMALLINT NULL COMMENT "",
    p_promo_id               VARCHAR(16) NULL COMMENT "",
    p_start_date_sk          LARGEINT NULL COMMENT "",
    p_end_date_sk            LARGEINT NULL COMMENT "",
    p_item_sk                LARGEINT NULL COMMENT "",
    p_cost                   FLOAT NULL COMMENT "",
    p_response_target        TINYINT NULL COMMENT "",
    p_promo_name             VARCHAR(5) NULL COMMENT "",
    p_channel_dmail          VARCHAR(1) NULL COMMENT "",
    p_channel_email          VARCHAR(1) NULL COMMENT "",
    p_channel_catalog        VARCHAR(1) NULL COMMENT "",
    p_channel_tv             VARCHAR(1) NULL COMMENT "",
    p_channel_radio          VARCHAR(1) NULL COMMENT "",
    p_channel_press          VARCHAR(1) NULL COMMENT "",
    p_channel_event          VARCHAR(1) NULL COMMENT "",
    p_channel_demo           VARCHAR(1) NULL COMMENT "",
    p_channel_details        VARCHAR(60) NULL COMMENT "",
    p_purpose                VARCHAR(7) NULL COMMENT "",
    p_discount_active        VARCHAR(1) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`p_promo_sk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`p_promo_sk`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "tpcdsq",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE IF NOT EXISTS catalog_page
(
    cp_catalog_page_sk       SMALLINT NULL COMMENT "",
    cp_catalog_page_id       VARCHAR(16) NULL COMMENT "",
    cp_start_date_sk         LARGEINT NULL COMMENT "",
    cp_end_date_sk           LARGEINT NULL COMMENT "",
    cp_department            VARCHAR(10) NULL COMMENT "",
    cp_catalog_number        TINYINT NULL COMMENT "",
    cp_catalog_page_number   SMALLINT NULL COMMENT "",
    cp_description           VARCHAR(99) NULL COMMENT "",
    cp_type                  VARCHAR(9) NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`cp_catalog_page_sk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`cp_catalog_page_sk`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "tpcdsr",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE IF NOT EXISTS inventory
(
    inv_date_sk              LARGEINT NULL COMMENT "",
    inv_item_sk              LARGEINT NULL COMMENT "",
    inv_warehouse_sk         TINYINT NULL COMMENT "",
    inv_quantity_on_hand     SMALLINT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`inv_date_sk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`inv_date_sk`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "tpcdss",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE IF NOT EXISTS catalog_returns
(
    cr_returned_date_sk       LARGEINT NULL COMMENT "",
    cr_returned_time_sk       LARGEINT NULL COMMENT "",
    cr_item_sk                LARGEINT NULL COMMENT "",
    cr_refunded_customer_sk   LARGEINT NULL COMMENT "",
    cr_refunded_cdemo_sk      LARGEINT NULL COMMENT "",
    cr_refunded_hdemo_sk      SMALLINT NULL COMMENT "",
    cr_refunded_addr_sk       LARGEINT NULL COMMENT "",
    cr_returning_customer_sk  LARGEINT NULL COMMENT "",
    cr_returning_cdemo_sk     LARGEINT NULL COMMENT "",
    cr_returning_hdemo_sk     SMALLINT NULL COMMENT "",
    cr_returning_addr_sk      LARGEINT NULL COMMENT "",
    cr_call_center_sk         TINYINT NULL COMMENT "",
    cr_catalog_page_sk        SMALLINT NULL COMMENT "",
    cr_ship_mode_sk           TINYINT NULL COMMENT "",
    cr_warehouse_sk           TINYINT NULL COMMENT "",
    cr_reason_sk              TINYINT NULL COMMENT "",
    cr_order_number           LARGEINT NULL COMMENT "",
    cr_return_quantity        TINYINT NULL COMMENT "",
    cr_return_amount          FLOAT NULL COMMENT "",
    cr_return_tax             FLOAT NULL COMMENT "",
    cr_return_amt_inc_tax     FLOAT NULL COMMENT "",
    cr_fee                    FLOAT NULL COMMENT "",
    cr_return_ship_cost       FLOAT NULL COMMENT "",
    cr_refunded_cash          FLOAT NULL COMMENT "",
    cr_reversed_charge        FLOAT NULL COMMENT "",
    cr_store_credit           FLOAT NULL COMMENT "",
    cr_net_loss               FLOAT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`cr_returned_date_sk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`cr_returned_date_sk`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "tpcdst",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE IF NOT EXISTS web_returns
(
    wr_returned_date_sk       LARGEINT NULL COMMENT "",
    wr_returned_time_sk       LARGEINT NULL COMMENT "",
    wr_item_sk                LARGEINT NULL COMMENT "",
    wr_refunded_customer_sk   LARGEINT NULL COMMENT "",
    wr_refunded_cdemo_sk      LARGEINT NULL COMMENT "",
    wr_refunded_hdemo_sk      SMALLINT NULL COMMENT "",
    wr_refunded_addr_sk       LARGEINT NULL COMMENT "",
    wr_returning_customer_sk  LARGEINT NULL COMMENT "",
    wr_returning_cdemo_sk     LARGEINT NULL COMMENT "",
    wr_returning_hdemo_sk     SMALLINT NULL COMMENT "",
    wr_returning_addr_sk      LARGEINT NULL COMMENT "",
    wr_web_page_sk            SMALLINT NULL COMMENT "",
    wr_reason_sk              TINYINT NULL COMMENT "",
    wr_order_number           LARGEINT NULL COMMENT "",
    wr_return_quantity        TINYINT NULL COMMENT "",
    wr_return_amt             FLOAT NULL COMMENT "",
    wr_return_tax             FLOAT NULL COMMENT "",
    wr_return_amt_inc_tax     FLOAT NULL COMMENT "",
    wr_fee                    FLOAT NULL COMMENT "",
    wr_return_ship_cost       FLOAT NULL COMMENT "",
    wr_refunded_cash          FLOAT NULL COMMENT "",
    wr_reversed_charge        FLOAT NULL COMMENT "",
    wr_account_credit         FLOAT NULL COMMENT "",
    wr_net_loss               FLOAT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`wr_returned_date_sk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`wr_returned_date_sk`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "tpcdsu",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE IF NOT EXISTS web_sales
(
    ws_sold_date_sk           LARGEINT NULL COMMENT "",
    ws_sold_time_sk           LARGEINT NULL COMMENT "",
    ws_ship_date_sk           LARGEINT NULL COMMENT "",
    ws_item_sk                LARGEINT NULL COMMENT "",
    ws_bill_customer_sk       LARGEINT NULL COMMENT "",
    ws_bill_cdemo_sk          LARGEINT NULL COMMENT "",
    ws_bill_hdemo_sk          SMALLINT NULL COMMENT "",
    ws_bill_addr_sk           LARGEINT NULL COMMENT "",
    ws_ship_customer_sk       LARGEINT NULL COMMENT "",
    ws_ship_cdemo_sk          LARGEINT NULL COMMENT "",
    ws_ship_hdemo_sk          SMALLINT NULL COMMENT "",
    ws_ship_addr_sk           LARGEINT NULL COMMENT "",
    ws_web_page_sk            SMALLINT NULL COMMENT "",
    ws_web_site_sk            TINYINT NULL COMMENT "",
    ws_ship_mode_sk           TINYINT NULL COMMENT "",
    ws_warehouse_sk           TINYINT NULL COMMENT "",
    ws_promo_sk               SMALLINT NULL COMMENT "",
    ws_order_number           LARGEINT NULL COMMENT "",
    ws_quantity               TINYINT NULL COMMENT "",
    ws_wholesale_cost         FLOAT NULL COMMENT "",
    ws_list_price             FLOAT NULL COMMENT "",
    ws_sales_price            FLOAT NULL COMMENT "",
    ws_ext_discount_amt       FLOAT NULL COMMENT "",
    ws_ext_sales_price        FLOAT NULL COMMENT "",
    ws_ext_wholesale_cost     FLOAT NULL COMMENT "",
    ws_ext_list_price         FLOAT NULL COMMENT "",
    ws_ext_tax                FLOAT NULL COMMENT "",
    ws_coupon_amt             FLOAT NULL COMMENT "",
    ws_ext_ship_cost          FLOAT NULL COMMENT "",
    ws_net_paid               FLOAT NULL COMMENT "",
    ws_net_paid_inc_tax       FLOAT NULL COMMENT "",
    ws_net_paid_inc_ship      FLOAT NULL COMMENT "",
    ws_net_paid_inc_ship_tax  FLOAT NULL COMMENT "",
    ws_net_profit             FLOAT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`ws_sold_date_sk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`ws_sold_date_sk`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "tpcdsv",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE IF NOT EXISTS catalog_sales
(
    cs_sold_date_sk           LARGEINT NULL COMMENT "",
    cs_sold_time_sk           LARGEINT NULL COMMENT "",
    cs_ship_date_sk           LARGEINT NULL COMMENT "",
    cs_bill_customer_sk       LARGEINT NULL COMMENT "",
    cs_bill_cdemo_sk          LARGEINT NULL COMMENT "",
    cs_bill_hdemo_sk          SMALLINT NULL COMMENT "",
    cs_bill_addr_sk           LARGEINT NULL COMMENT "",
    cs_ship_customer_sk       LARGEINT NULL COMMENT "",
    cs_ship_cdemo_sk          LARGEINT NULL COMMENT "",
    cs_ship_hdemo_sk          SMALLINT NULL COMMENT "",
    cs_ship_addr_sk           LARGEINT NULL COMMENT "",
    cs_call_center_sk         TINYINT NULL COMMENT "",
    cs_catalog_page_sk        SMALLINT NULL COMMENT "",
    cs_ship_mode_sk           TINYINT NULL COMMENT "",
    cs_warehouse_sk           TINYINT NULL COMMENT "",
    cs_item_sk                LARGEINT NULL COMMENT "",
    cs_promo_sk               SMALLINT NULL COMMENT "",
    cs_order_number           LARGEINT NULL COMMENT "",
    cs_quantity               TINYINT NULL COMMENT "",
    cs_wholesale_cost         FLOAT NULL COMMENT "",
    cs_list_price             FLOAT NULL COMMENT "",
    cs_sales_price            FLOAT NULL COMMENT "",
    cs_ext_discount_amt       FLOAT NULL COMMENT "",
    cs_ext_sales_price        FLOAT NULL COMMENT "",
    cs_ext_wholesale_cost     FLOAT NULL COMMENT "",
    cs_ext_list_price         FLOAT NULL COMMENT "",
    cs_ext_tax                FLOAT NULL COMMENT "",
    cs_coupon_amt             FLOAT NULL COMMENT "",
    cs_ext_ship_cost          FLOAT NULL COMMENT "",
    cs_net_paid               FLOAT NULL COMMENT "",
    cs_net_paid_inc_tax       FLOAT NULL COMMENT "",
    cs_net_paid_inc_ship      FLOAT NULL COMMENT "",
    cs_net_paid_inc_ship_tax  FLOAT NULL COMMENT "",
    cs_net_profit             FLOAT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`cs_sold_date_sk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`cs_sold_date_sk`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "tpcdsw",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);

CREATE TABLE IF NOT EXISTS store_sales
(
    ss_sold_date_sk           LARGEINT NULL COMMENT "",
    ss_sold_time_sk           LARGEINT NULL COMMENT "",
    ss_item_sk                LARGEINT NULL COMMENT "",
    ss_customer_sk            LARGEINT NULL COMMENT "",
    ss_cdemo_sk               LARGEINT NULL COMMENT "",
    ss_hdemo_sk               SMALLINT NULL COMMENT "",
    ss_addr_sk                LARGEINT NULL COMMENT "",
    ss_store_sk               SMALLINT NULL COMMENT "",
    ss_promo_sk               SMALLINT NULL COMMENT "",
    ss_ticket_number          LARGEINT NULL COMMENT "",
    ss_quantity               TINYINT NULL COMMENT "",
    ss_wholesale_cost         FLOAT NULL COMMENT "",
    ss_list_price             FLOAT NULL COMMENT "",
    ss_sales_price            FLOAT NULL COMMENT "",
    ss_ext_discount_amt       FLOAT NULL COMMENT "",
    ss_ext_sales_price        FLOAT NULL COMMENT "",
    ss_ext_wholesale_cost     FLOAT NULL COMMENT "",
    ss_ext_list_price         FLOAT NULL COMMENT "",
    ss_ext_tax                FLOAT NULL COMMENT "",
    ss_coupon_amt             FLOAT NULL COMMENT "",
    ss_net_paid               FLOAT NULL COMMENT "",
    ss_net_paid_inc_tax       FLOAT NULL COMMENT "",
    ss_net_profit             FLOAT NULL COMMENT ""
) ENGINE=OLAP
DUPLICATE KEY(`ss_sold_date_sk`)
COMMENT "OLAP"
DISTRIBUTED BY HASH(`ss_sold_date_sk`) BUCKETS 1
PROPERTIES (
  "replication_num" = "1",
  "colocate_with" = "tpcdsx",
  "in_memory" = "false",
  "storage_format" = "DEFAULT"
);