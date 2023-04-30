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

WITH top_items AS (
  SELECT cs_item_sk,
                cs_warehouse_sk,
                d_week_seq,
                sum(CASE
                      WHEN p_promo_sk IS NULL THEN 1
                      ELSE 0
                    END) no_promo,
                sum(CASE
                      WHEN p_promo_sk IS NOT NULL THEN 1
                      ELSE 0
                    END) promo,
                count(*) total_cnt
  FROM   catalog_sales
        JOIN inventory
          ON ( cs_item_sk = inv_item_sk and cs_sold_date_sk = inv_date_sk and cs_warehouse_sk = inv_warehouse_sk )
        JOIN customer_demographics
          ON ( cs_bill_cdemo_sk = cd_demo_sk )
        JOIN household_demographics
          ON ( cs_bill_hdemo_sk = hd_demo_sk )
        JOIN date_dim d1
          ON ( cs_sold_date_sk = d1.d_date_sk )
        LEFT OUTER JOIN promotion
                      ON ( cs_promo_sk = p_promo_sk )
        LEFT OUTER JOIN catalog_returns
                      ON ( cr_item_sk = cs_item_sk
                          AND cr_order_number = cs_order_number )
  WHERE inv_quantity_on_hand < cs_quantity
        AND hd_buy_potential = '501-1000'
        AND d1.d_year = 2002
        AND cd_marital_status = 'M'
  GROUP  BY cs_item_sk, cs_warehouse_sk, d1.d_week_seq
)
SELECT i_item_desc,
       w_warehouse_name,
       top_items.d_week_seq,
       no_promo,
       promo,
       total_cnt
FROM top_items
JOIN warehouse
  ON ( w_warehouse_sk = cs_warehouse_sk )
JOIN item
  ON ( i_item_sk = cs_item_sk )
ORDER  BY total_cnt DESC,
          1, 2, 3
LIMIT 100 SETTINGS distributed_product_mode = 'global', partial_merge_join_optimizations = 1, max_bytes_before_external_group_by = 50000000000, max_bytes_before_external_sort = 50000000000;
