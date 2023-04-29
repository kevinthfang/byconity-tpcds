-- 
-- Legal Notice 
-- 
-- This document and associated source code (the "Work") is a part of a 
-- benchmark specification maintained by the TPC. 
-- 
-- The TPC reserves all right, title, and interest to the Work as provided 
-- under U.S. and international laws, including without limitation all patent 
-- and trademark rights therein. 
-- 
-- No Warranty 
-- 
-- 1.1 TO THE MAXIMUM EXTENT PERMITTED BY APPLICABLE LAW, THE INFORMATION 
--     CONTAINED HEREIN IS PROVIDED "AS IS" AND WITH ALL FAULTS, AND THE 
--     AUTHORS AND DEVELOPERS OF THE WORK HEREBY DISCLAIM ALL OTHER 
--     WARRANTIES AND CONDITIONS, EITHER EXPRESS, IMPLIED OR STATUTORY, 
--     INCLUDING, BUT NOT LIMITED TO, ANY (IF ANY) IMPLIED WARRANTIES, 
--     DUTIES OR CONDITIONS OF MERCHANTABILITY, OF FITNESS FOR A PARTICULAR 
--     PURPOSE, OF ACCURACY OR COMPLETENESS OF RESPONSES, OF RESULTS, OF 
--     WORKMANLIKE EFFORT, OF LACK OF VIRUSES, AND OF LACK OF NEGLIGENCE. 
--     ALSO, THERE IS NO WARRANTY OR CONDITION OF TITLE, QUIET ENJOYMENT, 
--     QUIET POSSESSION, CORRESPONDENCE TO DESCRIPTION OR NON-INFRINGEMENT 
--     WITH REGARD TO THE WORK. 
-- 1.2 IN NO EVENT WILL ANY AUTHOR OR DEVELOPER OF THE WORK BE LIABLE TO 
--     ANY OTHER PARTY FOR ANY DAMAGES, INCLUDING BUT NOT LIMITED TO THE 
--     COST OF PROCURING SUBSTITUTE GOODS OR SERVICES, LOST PROFITS, LOSS 
--     OF USE, LOSS OF DATA, OR ANY INCIDENTAL, CONSEQUENTIAL, DIRECT, 
--     INDIRECT, OR SPECIAL DAMAGES WHETHER UNDER CONTRACT, TORT, WARRANTY,
--     OR OTHERWISE, ARISING IN ANY WAY OUT OF THIS OR ANY OTHER AGREEMENT 
--     RELATING TO THE WORK, WHETHER OR NOT SUCH AUTHOR OR DEVELOPER HAD 
--     ADVANCE NOTICE OF THE POSSIBILITY OF SUCH DAMAGES. 
-- 
-- Contributors:
-- Gradient Systems
--
DROP DATABASE IF EXISTS tpcds_local ON CLUSTER ch_benchmark;
CREATE DATABASE tpcds_local ON CLUSTER ch_benchmark;
USE tpcds_local;

CREATE TABLE customer_address ON CLUSTER ch_benchmark
(
    ca_address_sk Int32,
    ca_address_id String,
    ca_street_number LowCardinality(Nullable(String)),
    ca_street_name LowCardinality(Nullable(String)),
    ca_street_type LowCardinality(Nullable(String)),
    ca_suite_number LowCardinality(Nullable(String)),
    ca_city LowCardinality(Nullable(String)),
    ca_county LowCardinality(Nullable(String)),
    ca_state FixedString(2),
    ca_zip LowCardinality(Nullable(String)),
    ca_country LowCardinality(Nullable(String)),
    ca_gmt_offset Nullable(Float32),
    ca_location_type LowCardinality(Nullable(String))
) ENGINE=MergeTree() ORDER BY (ca_address_sk);

CREATE TABLE customer_demographics ON CLUSTER ch_benchmark
(
    cd_demo_sk Int32,
    cd_gender FixedString(1),
    cd_marital_status FixedString(1),
    cd_education_status LowCardinality(String),
    cd_purchase_estimate Int16,
    cd_credit_rating LowCardinality(String),
    cd_dep_count Int8,
    cd_dep_employed_count Int8,
    cd_dep_college_count Int8
) ENGINE=MergeTree() ORDER BY (cd_demo_sk);

CREATE TABLE date_dim ON CLUSTER ch_benchmark
(
    d_date_sk Int32,
    d_date_id String,
    d_date Date32,
    d_month_seq Int16,
    d_week_seq Int16,
    d_quarter_seq Int16,
    d_year Int16,
    d_dow Int8,
    d_moy Int8,
    d_dom Int8,
    d_qoy Int8,
    d_fy_year Int16,
    d_fy_quarter_seq Int16,
    d_fy_week_seq Int16,
    d_day_name LowCardinality(String),
    d_quarter_name LowCardinality(String),
    d_holiday FixedString(1),
    d_weekend FixedString(1),
    d_following_holiday FixedString(1),
    d_first_dom Int32,
    d_last_dom Int32,
    d_same_day_ly Int32,
    d_same_day_lq Int32,
    d_current_day FixedString(1),
    d_current_week FixedString(1),
    d_current_month FixedString(1),
    d_current_quarter FixedString(1),
    d_current_year FixedString(1)
) ENGINE=MergeTree() ORDER BY (d_date_sk);

CREATE TABLE warehouse ON CLUSTER ch_benchmark
(
    w_warehouse_sk Int8,
    w_warehouse_id String,
    w_warehouse_name Nullable(String),
    w_warehouse_sq_ft Nullable(Int32),
    w_street_number Nullable(String),
    w_street_name Nullable(String),
    w_street_type Nullable(String),
    w_suite_number Nullable(String),
    w_city String,
    w_county String,
    w_state FixedString(2),
    w_zip String,
    w_country LowCardinality(String),
    w_gmt_offset Nullable(Float32)
) ENGINE=MergeTree() ORDER BY (w_warehouse_sk);

CREATE TABLE ship_mode ON CLUSTER ch_benchmark
(
    sm_ship_mode_sk Int8,
    sm_ship_mode_id String,
    sm_type String,
    sm_code LowCardinality(String),
    sm_carrier String,
    sm_contract String
) ENGINE=MergeTree() ORDER BY (sm_ship_mode_sk);

CREATE TABLE time_dim ON CLUSTER ch_benchmark
(
    t_time_sk Int32,
    t_time_id String,
    t_time Int32,
    t_hour Int8,
    t_minute Int8,
    t_second Int8,
    t_am_pm FixedString(2),
    t_shift LowCardinality(String),
    t_sub_shift LowCardinality(String),
    t_meal_time LowCardinality(Nullable(String))
) ENGINE=MergeTree() ORDER BY (t_time_sk);

CREATE TABLE reason ON CLUSTER ch_benchmark
(
    r_reason_sk Int8,
    r_reason_id String,
    r_reason_desc String
) ENGINE=MergeTree() ORDER BY (r_reason_sk);

CREATE TABLE income_band ON CLUSTER ch_benchmark
(
    ib_income_band_sk Int8,
    ib_lower_bound Int32,
    ib_upper_bound Int32
) ENGINE=MergeTree() ORDER BY (ib_income_band_sk);

CREATE TABLE item ON CLUSTER ch_benchmark
(
    i_item_sk Int32,
    i_item_id String,
    i_rec_start_date Nullable(Date),
    i_rec_end_date Nullable(Date),
    i_item_desc Nullable(String),
    i_current_price Nullable(Float32),
    i_wholesale_cost Nullable(Float32),
    i_brand_id Nullable(Int32),
    i_brand LowCardinality(Nullable(String)),
    i_class_id Nullable(Int8),
    i_class LowCardinality(Nullable(String)),
    i_category_id Nullable(Int8),
    i_category LowCardinality(Nullable(String)),
    i_manufact_id Nullable(Int16),
    i_manufact LowCardinality(Nullable(String)),
    i_size LowCardinality(Nullable(String)),
    i_formulation Nullable(String),
    i_color LowCardinality(Nullable(String)),
    i_units LowCardinality(Nullable(String)),
    i_container LowCardinality(Nullable(String)),
    i_manager_id Nullable(Int8),
    i_product_name Nullable(String)
) ENGINE=MergeTree() ORDER BY (i_item_sk);

CREATE TABLE store ON CLUSTER ch_benchmark
(
    s_store_sk Int16,
    s_store_id String,
    s_rec_start_date Nullable(Date),
    s_rec_end_date Nullable(Date),
    s_closed_date_sk Nullable(Int32),
    s_store_name LowCardinality(Nullable(String)),
    s_number_employees Nullable(Int16),
    s_floor_space Nullable(Int32),
    s_hours LowCardinality(Nullable(String)),
    s_manager Nullable(String),
    s_market_id Nullable(Int8),
    s_geography_class LowCardinality(Nullable(String)),
    s_market_desc Nullable(String),
    s_market_manager Nullable(String),
    s_division_id Nullable(Int8),
    s_division_name LowCardinality(Nullable(String)),
    s_company_id Nullable(Int8),
    s_company_name LowCardinality(Nullable(String)),
    s_street_number Nullable(String),
    s_street_name Nullable(String),
    s_street_type LowCardinality(Nullable(String)),
    s_suite_number LowCardinality(Nullable(String)),
    s_city LowCardinality(Nullable(String)),
    s_county LowCardinality(Nullable(String)),
    s_state FixedString(2),
    s_zip Nullable(String),
    s_country LowCardinality(Nullable(String)),
    s_gmt_offset Nullable(Float32),
    s_tax_precentage Nullable(Float32)
) ENGINE=MergeTree() ORDER BY (s_store_sk);

CREATE TABLE call_center ON CLUSTER ch_benchmark
(
    cc_call_center_sk Int8,
    cc_call_center_id String,
    cc_rec_start_date Date,
    cc_rec_end_date Nullable(Date),
    cc_closed_date_sk Nullable(Int32),
    cc_open_date_sk Int32,
    cc_name String,
    cc_class LowCardinality(String),
    cc_employees Int32,
    cc_sq_ft Int32,
    cc_hours LowCardinality(String),
    cc_manager String,
    cc_mkt_id Int8,
    cc_mkt_class String,
    cc_mkt_desc String,
    cc_market_manager String,
    cc_division Int8,
    cc_division_name LowCardinality(String),
    cc_company Int8,
    cc_company_name LowCardinality(String),
    cc_street_number String,
    cc_street_name String,
    cc_street_type String,
    cc_suite_number String,
    cc_city Nullable(String),
    cc_county String,
    cc_state FixedString(2),
    cc_zip String,
    cc_country LowCardinality(String),
    cc_gmt_offset Float32,
    cc_tax_percentage Float32
) ENGINE=MergeTree() ORDER BY (cc_call_center_sk);

CREATE TABLE customer ON CLUSTER ch_benchmark
(
    c_customer_sk Int32,
    c_customer_id String,
    c_current_cdemo_sk Nullable(Int32),
    c_current_hdemo_sk Nullable(Int16),
    c_current_addr_sk Int32,
    c_first_shipto_date_sk Nullable(Int32),
    c_first_sales_date_sk Nullable(Int32),
    c_salutation LowCardinality(Nullable(String)),
    c_first_name Nullable(FixedString(11)),
    c_last_name Nullable(FixedString(13)),
    c_preferred_cust_flag FixedString(1),
    c_birth_day Nullable(Int8),
    c_birth_month Nullable(Int8),
    c_birth_year Nullable(Int16),
    c_birth_country LowCardinality(Nullable(String)),
    c_login Nullable(String),
    c_email_address Nullable(String),
    c_last_review_date LowCardinality(Nullable(String))
) ENGINE=MergeTree() ORDER BY (c_customer_sk);

CREATE TABLE web_site ON CLUSTER ch_benchmark
(
    web_site_sk Int8,
    web_site_id String,
    web_rec_start_date Nullable(Date),
    web_rec_end_date Nullable(Date),
    web_name LowCardinality(Nullable(String)),
    web_open_date_sk Nullable(Int32),
    web_close_date_sk Nullable(Int32),
    web_class LowCardinality(Nullable(String)),
    web_manager Nullable(String),
    web_mkt_id Nullable(Int8),
    web_mkt_class Nullable(String),
    web_mkt_desc String,
    web_market_manager Nullable(String),
    web_company_id Int8,
    web_company_name LowCardinality(Nullable(String)),
    web_street_number Nullable(String),
    web_street_name Nullable(String),
    web_street_type String,
    web_suite_number String,
    web_city Nullable(String),
    web_county Nullable(String),
    web_state FixedString(2),
    web_zip String,
    web_country LowCardinality(Nullable(String)),
    web_gmt_offset Nullable(Float32),
    web_tax_percentage Nullable(Float32)
) ENGINE=MergeTree() ORDER BY (web_site_sk);

CREATE TABLE store_returns ON CLUSTER ch_benchmark
(
    sr_returned_date_sk Nullable(Int32),
    sr_return_time_sk Nullable(Int32),
    sr_item_sk Int32,
    sr_customer_sk Nullable(Int32),
    sr_cdemo_sk Nullable(Int32),
    sr_hdemo_sk Nullable(Int16),
    sr_addr_sk Nullable(Int32),
    sr_store_sk Nullable(Int16),
    sr_reason_sk Nullable(Int8),
    sr_ticket_number Int64,
    sr_return_quantity Nullable(Int8),
    sr_return_amt Nullable(Float32),
    sr_return_tax Nullable(Float32),
    sr_return_amt_inc_tax Nullable(Float32),
    sr_fee Nullable(Float32),
    sr_return_ship_cost Nullable(Float32),
    sr_refunded_cash Nullable(Float32),
    sr_reversed_charge Nullable(Float32),
    sr_store_credit Nullable(Float32),
    sr_net_loss Nullable(Float32)
) ENGINE = MergeTree() ORDER BY (sr_item_sk, sr_ticket_number);

CREATE TABLE household_demographics ON CLUSTER ch_benchmark
(
    hd_demo_sk Int16,
    hd_income_band_sk Int8,
    hd_buy_potential LowCardinality(String),
    hd_dep_count Int8,
    hd_vehicle_count Int8
) ENGINE = MergeTree() ORDER BY (hd_demo_sk);

CREATE TABLE web_page ON CLUSTER ch_benchmark
(
    wp_web_page_sk Int16,
    wp_web_page_id String,
    wp_rec_start_date Nullable(Date),
    wp_rec_end_date Nullable(Date),
    wp_creation_date_sk Nullable(Int32),
    wp_access_date_sk Nullable(Int32),
    wp_autogen_flag FixedString(1),
    wp_customer_sk Nullable(Int32),
    wp_url LowCardinality(Nullable(String)),
    wp_type LowCardinality(Nullable(String)),
    wp_char_count Nullable(Int16),
    wp_link_count Nullable(Int8),
    wp_image_count Nullable(Int8),
    wp_max_ad_count Nullable(Int8)
) ENGINE = MergeTree() ORDER BY (wp_web_page_sk);

CREATE TABLE promotion ON CLUSTER ch_benchmark
(
    p_promo_sk Int16,
    p_promo_id String,
    p_start_date_sk Nullable(Int32),
    p_end_date_sk Nullable(Int32),
    p_item_sk Nullable(Int32),
    p_cost Nullable(Float64),
    p_response_target Nullable(Int8),
    p_promo_name LowCardinality(Nullable(String)),
    p_channel_dmail FixedString(1),
    p_channel_email FixedString(1),
    p_channel_catalog FixedString(1),
    p_channel_tv FixedString(1),
    p_channel_radio FixedString(1),
    p_channel_press FixedString(1),
    p_channel_event FixedString(1),
    p_channel_demo FixedString(1),
    p_channel_details Nullable(String),
    p_purpose LowCardinality(Nullable(String)),
    p_discount_active FixedString(1)
) ENGINE = MergeTree() ORDER BY (p_promo_sk);

CREATE TABLE catalog_page ON CLUSTER ch_benchmark
(
    cp_catalog_page_sk Int16,
    cp_catalog_page_id String,
    cp_start_date_sk Nullable(Int32),
    cp_end_date_sk Nullable(Int32),
    cp_department LowCardinality(Nullable(String)),
    cp_catalog_number Nullable(Int8),
    cp_catalog_page_number Nullable(Int16),
    cp_description Nullable(String),
    cp_type LowCardinality(Nullable(String))
) ENGINE = MergeTree() ORDER BY (cp_catalog_page_sk);

CREATE TABLE inventory ON CLUSTER ch_benchmark
(
    inv_date_sk Int32,
    inv_item_sk Int32,
    inv_warehouse_sk Int8,
    inv_quantity_on_hand Nullable(Int16)
) ENGINE = MergeTree() ORDER BY (inv_date_sk, inv_item_sk, inv_warehouse_sk);

CREATE TABLE catalog_returns ON CLUSTER ch_benchmark
(
    cr_returned_date_sk Int32,
    cr_returned_time_sk Int32,
    cr_item_sk Int32,
    cr_refunded_customer_sk Nullable(Int32),
    cr_refunded_cdemo_sk Nullable(Int32),
    cr_refunded_hdemo_sk Nullable(Int16),
    cr_refunded_addr_sk Nullable(Int32),
    cr_returning_customer_sk Nullable(Int32),
    cr_returning_cdemo_sk Nullable(Int32),
    cr_returning_hdemo_sk Nullable(Int16),
    cr_returning_addr_sk Nullable(Int32),
    cr_call_center_sk Nullable(Int8),
    cr_catalog_page_sk Nullable(Int16),
    cr_ship_mode_sk Nullable(Int8),
    cr_warehouse_sk Nullable(Int8),
    cr_reason_sk Nullable(Int8),
    cr_order_number Int64,
    cr_return_quantity Nullable(Int8),
    cr_return_amount Nullable(Float32),
    cr_return_tax Nullable(Float32),
    cr_return_amt_inc_tax Nullable(Float32),
    cr_fee Nullable(Float32),
    cr_return_ship_cost Nullable(Float32),
    cr_refunded_cash Nullable(Float32),
    cr_reversed_charge Nullable(Float32),
    cr_store_credit Nullable(Float32),
    cr_net_loss Nullable(Float32)
) ENGINE = MergeTree() ORDER BY (cr_item_sk, cr_order_number);

CREATE TABLE web_returns ON CLUSTER ch_benchmark
(
    wr_returned_date_sk Nullable(Int32),
    wr_returned_time_sk Nullable(Int32),
    wr_item_sk Int32,
    wr_refunded_customer_sk Nullable(Int32),
    wr_refunded_cdemo_sk Nullable(Int32),
    wr_refunded_hdemo_sk Nullable(Int16),
    wr_refunded_addr_sk Nullable(Int32),
    wr_returning_customer_sk Nullable(Int32),
    wr_returning_cdemo_sk Nullable(Int32),
    wr_returning_hdemo_sk Nullable(Int16),
    wr_returning_addr_sk Nullable(Int32),
    wr_web_page_sk Nullable(Int16),
    wr_reason_sk Nullable(Int8),
    wr_order_number Int64,
    wr_return_quantity Nullable(Int8),
    wr_return_amt Nullable(Float32),
    wr_return_tax Nullable(Float32),
    wr_return_amt_inc_tax Nullable(Float32),
    wr_fee Nullable(Float32),
    wr_return_ship_cost Nullable(Float32),
    wr_refunded_cash Nullable(Float32),
    wr_reversed_charge Nullable(Float32),
    wr_account_credit Nullable(Float32),
    wr_net_loss Nullable(Float32)
) ENGINE = MergeTree() ORDER BY (wr_item_sk, wr_order_number);

CREATE TABLE web_sales ON CLUSTER ch_benchmark
(
    ws_sold_date_sk Nullable(Int32),
    ws_sold_time_sk Nullable(Int32),
    ws_ship_date_sk Nullable(Int32),
    ws_item_sk Int32,
    ws_bill_customer_sk Nullable(Int32),
    ws_bill_cdemo_sk Nullable(Int32),
    ws_bill_hdemo_sk Nullable(Int16),
    ws_bill_addr_sk Nullable(Int32),
    ws_ship_customer_sk Nullable(Int32),
    ws_ship_cdemo_sk Nullable(Int32),
    ws_ship_hdemo_sk Nullable(Int16),
    ws_ship_addr_sk Nullable(Int32),
    ws_web_page_sk Nullable(Int16),
    ws_web_site_sk Nullable(Int8),
    ws_ship_mode_sk Nullable(Int8),
    ws_warehouse_sk Nullable(Int8),
    ws_promo_sk Nullable(Int16),
    ws_order_number Int64,
    ws_quantity Nullable(Int8),
    ws_wholesale_cost Nullable(Float32),
    ws_list_price Nullable(Float32),
    ws_sales_price Nullable(Float32),
    ws_ext_discount_amt Nullable(Float32),
    ws_ext_sales_price Nullable(Float32),
    ws_ext_wholesale_cost Nullable(Float32),
    ws_ext_list_price Nullable(Float32),
    ws_ext_tax Nullable(Float32),
    ws_coupon_amt Nullable(Float32),
    ws_ext_ship_cost Nullable(Float32),
    ws_net_paid Nullable(Float32),
    ws_net_paid_inc_tax Nullable(Float32),
    ws_net_paid_inc_ship Float32,
    ws_net_paid_inc_ship_tax Float32,
    ws_net_profit Float32
) ENGINE = MergeTree() ORDER BY (ws_sold_date_sk);

CREATE TABLE catalog_sales ON CLUSTER ch_benchmark
(
    cs_sold_date_sk Nullable(Int32),
    cs_sold_time_sk Nullable(Int32),
    cs_ship_date_sk Nullable(Int32),
    cs_bill_customer_sk Nullable(Int32),
    cs_bill_cdemo_sk Nullable(Int32),
    cs_bill_hdemo_sk Nullable(Int16),
    cs_bill_addr_sk Nullable(Int32),
    cs_ship_customer_sk Nullable(Int32),
    cs_ship_cdemo_sk Nullable(Int32),
    cs_ship_hdemo_sk Nullable(Int16),
    cs_ship_addr_sk Nullable(Int32),
    cs_call_center_sk Nullable(Int8),
    cs_catalog_page_sk Nullable(Int16),
    cs_ship_mode_sk Nullable(Int8),
    cs_warehouse_sk Nullable(Int8),
    cs_item_sk Int32,
    cs_promo_sk Nullable(Int16),
    cs_order_number Int64,
    cs_quantity Nullable(Int8),
    cs_wholesale_cost Nullable(Float32),
    cs_list_price Nullable(Float32),
    cs_sales_price Nullable(Float32),
    cs_ext_discount_amt Nullable(Float32),
    cs_ext_sales_price Nullable(Float32),
    cs_ext_wholesale_cost Nullable(Float32),
    cs_ext_list_price Nullable(Float32),
    cs_ext_tax Nullable(Float32),
    cs_coupon_amt Nullable(Float32),
    cs_ext_ship_cost Nullable(Float32),
    cs_net_paid Nullable(Float32),
    cs_net_paid_inc_tax Nullable(Float32),
    cs_net_paid_inc_ship Float32,
    cs_net_paid_inc_ship_tax Float32,
    cs_net_profit Float32
) ENGINE = MergeTree() ORDER BY (cs_sold_date_sk);

CREATE TABLE store_sales ON CLUSTER ch_benchmark
(
    ss_sold_date_sk Nullable(Int32),
    ss_sold_time_sk Nullable(Int32),
    ss_item_sk Int32,
    ss_customer_sk Nullable(Int32),
    ss_cdemo_sk Nullable(Int32),
    ss_hdemo_sk Nullable(Int16),
    ss_addr_sk Nullable(Int32),
    ss_store_sk Nullable(Int16),
    ss_promo_sk Nullable(Int16),
    ss_ticket_number Int64,
    ss_quantity Nullable(Int8),
    ss_wholesale_cost Nullable(Float32),
    ss_list_price Nullable(Float32),
    ss_sales_price Nullable(Float32),
    ss_ext_discount_amt Nullable(Float32),
    ss_ext_sales_price Nullable(Float32),
    ss_ext_wholesale_cost Nullable(Float32),
    ss_ext_list_price Nullable(Float32),
    ss_ext_tax Nullable(Float32),
    ss_coupon_amt Nullable(Float32),
    ss_net_paid Nullable(Float32),
    ss_net_paid_inc_tax Nullable(Float32),
    ss_net_profit Nullable(Float32)
) ENGINE = MergeTree() ORDER BY (ss_sold_date_sk);

-- Create distributed table
DROP DATABASE IF EXISTS tpcds ON CLUSTER ch_benchmark;
CREATE DATABASE tpcds ON CLUSTER ch_benchmark;
use tpcds;

CREATE TABLE customer_address ON CLUSTER ch_benchmark AS tpcds_local.customer_address
ENGINE=Distributed(ch_benchmark, tpcds_local, customer_address,      intHash64(ca_address_sk));
CREATE TABLE customer_demographics ON CLUSTER ch_benchmark AS tpcds_local.customer_demographics
ENGINE=Distributed(ch_benchmark, tpcds_local, customer_demographics, intHash64(cd_demo_sk));
CREATE TABLE date_dim ON CLUSTER ch_benchmark AS tpcds_local.date_dim
ENGINE=Distributed(ch_benchmark, tpcds_local, date_dim,              intHash64(d_date_sk));
CREATE TABLE warehouse ON CLUSTER ch_benchmark AS tpcds_local.warehouse
ENGINE=Distributed(ch_benchmark, tpcds_local, warehouse,             intHash64(w_warehouse_sk));
CREATE TABLE ship_mode ON CLUSTER ch_benchmark AS tpcds_local.ship_mode
ENGINE=Distributed(ch_benchmark, tpcds_local, ship_mode,             intHash64(sm_ship_mode_sk));
CREATE TABLE time_dim ON CLUSTER ch_benchmark AS tpcds_local.time_dim
ENGINE=Distributed(ch_benchmark, tpcds_local, time_dim,              intHash64(t_time_sk));
CREATE TABLE reason ON CLUSTER ch_benchmark AS tpcds_local.reason
ENGINE=Distributed(ch_benchmark, tpcds_local, reason,                intHash64(r_reason_sk));
CREATE TABLE income_band ON CLUSTER ch_benchmark AS tpcds_local.income_band
ENGINE=Distributed(ch_benchmark, tpcds_local, income_band,           intHash64(ib_income_band_sk));
CREATE TABLE item ON CLUSTER ch_benchmark AS tpcds_local.item
ENGINE=Distributed(ch_benchmark, tpcds_local, item,                  intHash64(i_item_sk));
CREATE TABLE store ON CLUSTER ch_benchmark AS tpcds_local.store
ENGINE=Distributed(ch_benchmark, tpcds_local, store,                 intHash64(s_store_sk));
CREATE TABLE call_center ON CLUSTER ch_benchmark AS tpcds_local.call_center
ENGINE=Distributed(ch_benchmark, tpcds_local, call_center,           intHash64(cc_call_center_sk));
CREATE TABLE customer ON CLUSTER ch_benchmark AS tpcds_local.customer
ENGINE=Distributed(ch_benchmark, tpcds_local, customer,              intHash64(c_customer_sk));
CREATE TABLE web_site ON CLUSTER ch_benchmark AS tpcds_local.web_site
ENGINE=Distributed(ch_benchmark, tpcds_local, web_site,              intHash64(web_site_sk));
CREATE TABLE store_returns ON CLUSTER ch_benchmark AS tpcds_local.store_returns
ENGINE=Distributed(ch_benchmark, tpcds_local, store_returns,         intHash64(sr_item_sk));
CREATE TABLE household_demographics ON CLUSTER ch_benchmark AS tpcds_local.household_demographics
ENGINE=Distributed(ch_benchmark, tpcds_local, household_demographics,intHash64(hd_demo_sk));
CREATE TABLE web_page ON CLUSTER ch_benchmark AS tpcds_local.web_page
ENGINE=Distributed(ch_benchmark, tpcds_local, web_page,              intHash64(wp_web_page_sk));
CREATE TABLE promotion ON CLUSTER ch_benchmark AS tpcds_local.promotion
ENGINE=Distributed(ch_benchmark, tpcds_local, promotion,             intHash64(p_promo_sk));
CREATE TABLE catalog_page ON CLUSTER ch_benchmark AS tpcds_local.catalog_page
ENGINE=Distributed(ch_benchmark, tpcds_local, catalog_page,          intHash64(cp_catalog_page_sk));
CREATE TABLE inventory ON CLUSTER ch_benchmark AS tpcds_local.inventory
ENGINE=Distributed(ch_benchmark, tpcds_local, inventory,             intHash64(inv_item_sk));
CREATE TABLE catalog_returns ON CLUSTER ch_benchmark AS tpcds_local.catalog_returns
ENGINE=Distributed(ch_benchmark, tpcds_local, catalog_returns,       intHash64(cr_item_sk));
CREATE TABLE web_returns ON CLUSTER ch_benchmark AS tpcds_local.web_returns
ENGINE=Distributed(ch_benchmark, tpcds_local, web_returns,           intHash64(wr_item_sk));
CREATE TABLE web_sales ON CLUSTER ch_benchmark AS tpcds_local.web_sales
ENGINE=Distributed(ch_benchmark, tpcds_local, web_sales,             intHash64(ws_item_sk));
CREATE TABLE catalog_sales ON CLUSTER ch_benchmark AS tpcds_local.catalog_sales
ENGINE=Distributed(ch_benchmark, tpcds_local, catalog_sales,         intHash64(cs_item_sk));
CREATE TABLE store_sales ON CLUSTER ch_benchmark AS tpcds_local.store_sales
ENGINE=Distributed(ch_benchmark, tpcds_local, store_sales,           intHash64(ss_item_sk));
