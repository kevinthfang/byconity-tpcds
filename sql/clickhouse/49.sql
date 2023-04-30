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

select channel,
    item,
    return_ratio,
    return_rank,
    currency_rank
FROM
(
SELECT 'web' AS channel,
               web.item,
               web.return_ratio,
               web.return_rank,
               web.currency_rank
FROM   (SELECT item,
               return_ratio,
               currency_ratio,
               rank()
                 OVER (
                   ORDER BY return_ratio)   AS return_rank,
               rank()
                 OVER (
                   ORDER BY currency_ratio) AS currency_rank
        FROM   (SELECT ws.ws_item_sk AS item,
                       ( sum(coalesce(wr.wr_return_quantity, 0)) /
                         sum(coalesce(ws.ws_quantity, 0)) ) AS
                       return_ratio,
                       ( sum(coalesce(wr.wr_return_amt, 0)) /
                         sum(coalesce(ws.ws_net_paid, 0)) ) AS currency_ratio
                FROM   web_sales ws
                       LEFT OUTER JOIN web_returns wr
                                    ON ( ws.ws_order_number = wr.wr_order_number
                                         AND ws.ws_item_sk = wr.wr_item_sk ),
                       date_dim
                WHERE  wr.wr_return_amt > 10000
                       AND ws.ws_net_profit > 1
                       AND ws.ws_net_paid > 0
                       AND ws.ws_quantity > 0
                       AND ws_sold_date_sk = d_date_sk
                       AND d_year = 1999
                       AND d_moy = 12
                GROUP  BY ws.ws_item_sk) in_web) web
WHERE  ( web.return_rank <= 10
          OR web.currency_rank <= 10 )
UNION ALL
SELECT 'catalog' AS channel,
       catalog.item,
       catalog.return_ratio,
       catalog.return_rank,
       catalog.currency_rank
FROM   (SELECT item,
               return_ratio,
               currency_ratio,
               rank()
                 OVER (
                   ORDER BY return_ratio)   AS return_rank,
               rank()
                 OVER (
                   ORDER BY currency_ratio) AS currency_rank
        FROM   (SELECT cs.cs_item_sk AS
                       item,
                       ( sum(coalesce(cr.cr_return_quantity, 0)) /
                         sum(coalesce(cs.cs_quantity, 0)) ) AS
                       return_ratio,
                       ( sum(coalesce(cr.cr_return_amount, 0)) /
                         sum(coalesce(cs.cs_net_paid, 0)) ) AS
                       currency_ratio
                FROM   catalog_sales cs
                       LEFT OUTER JOIN catalog_returns cr
                                    ON ( cs.cs_order_number = cr.cr_order_number
                                         AND cs.cs_item_sk = cr.cr_item_sk ),
                       date_dim
                WHERE  cr.cr_return_amount > 10000
                       AND cs.cs_net_profit > 1
                       AND cs.cs_net_paid > 0
                       AND cs.cs_quantity > 0
                       AND cs_sold_date_sk = d_date_sk
                       AND d_year = 1999
                       AND d_moy = 12
                GROUP  BY cs.cs_item_sk) in_cat) catalog
WHERE  ( catalog.return_rank <= 10
          OR catalog.currency_rank <= 10 )
UNION ALL
SELECT 'store' AS channel,
       store.item,
       store.return_ratio,
       store.return_rank,
       store.currency_rank
FROM   (SELECT item,
               return_ratio,
               currency_ratio,
               rank()
                 OVER (
                   ORDER BY return_ratio)   AS return_rank,
               rank()
                 OVER (
                   ORDER BY currency_ratio) AS currency_rank
        FROM   (SELECT sts.ss_item_sk AS
                       item,
                       ( sum(coalesce(sr.sr_return_quantity, 0)) /
                         sum(coalesce(sts.ss_quantity, 0)) ) AS
                       return_ratio,
                       ( sum(coalesce(sr.sr_return_amt, 0))
                         / sum(coalesce(sts.ss_net_paid, 0)) ) AS
                       currency_ratio
                FROM   store_sales sts
                       LEFT OUTER JOIN store_returns sr
                                    ON ( sts.ss_ticket_number =
                                         sr.sr_ticket_number
                                         AND sts.ss_item_sk = sr.sr_item_sk ),
                       date_dim
                WHERE  sr.sr_return_amt > 10000
                       AND sts.ss_net_profit > 1
                       AND sts.ss_net_paid > 0
                       AND sts.ss_quantity > 0
                       AND ss_sold_date_sk = d_date_sk
                       AND d_year = 1999
                       AND d_moy = 12
                GROUP  BY sts.ss_item_sk) in_store) store
WHERE  ( store.return_rank <= 10
          OR store.currency_rank <= 10 )
)
ORDER  BY 1,
          4,
          5
LIMIT 100 SETTINGS distributed_product_mode = 'global', partial_merge_join_optimizations = 1, max_bytes_before_external_group_by = 50000000000, max_bytes_before_external_sort = 50000000000; 
