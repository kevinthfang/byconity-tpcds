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

with ss_items as(
   select i_item_id item_id
        ,sum(ss_ext_sales_price) ss_item_rev
 from store_sales
     ,item
     ,date_dim
 where (ss_item_sk = i_item_sk)
   and (d_date in (
     select d_date
     from 
      date_dim
      where (d_week_seq = (
        select d_week_seq
        from 
          date_dim
        where (d_date ='2000-01-03')
        ))
    ))
   and ss_sold_date_sk   = d_date_sk
 group by i_item_id
 )
 ,cs_items as(
   select 
    i_item_id item_id
    ,sum(cs_ext_sales_price) cs_item_rev
  from 
      catalog_sales
      ,item
      ,date_dim
 where (cs_item_sk = i_item_sk)
  and  (d_date in (
      select d_date
      from 
        date_dim
       where (d_week_seq = (select d_week_seq
                                      from date_dim
                                      where (d_date = '2000-01-03')
        ))
    ))
  and  cs_sold_date_sk = d_date_sk
 group by i_item_id),
 ws_items as(
   select 
        i_item_id item_id
        ,sum(ws_ext_sales_price) ws_item_rev
  from web_sales
      ,item
      ,date_dim
 where (ws_item_sk = i_item_sk)
  and  (d_date in (
    select d_date
    from 
      date_dim
    where (d_week_seq =(
          select d_week_seq
          from 
            date_dim
          where (d_date = '2000-01-03')
        ))
      ))
  and ws_sold_date_sk   = d_date_sk
 group by i_item_id
 )
  select ss_items.item_id
       ,ss_item_rev
       ,ss_item_rev/((ss_item_rev+cs_item_rev+ws_item_rev)/3) * 100 ss_dev
       ,cs_item_rev
       ,cs_item_rev/((ss_item_rev+cs_item_rev+ws_item_rev)/3) * 100 cs_dev
       ,ws_item_rev
       ,ws_item_rev/((ss_item_rev+cs_item_rev+ws_item_rev)/3) * 100 ws_dev
       ,(ss_item_rev+cs_item_rev+ws_item_rev)/3 average
 from 
 ss_items
 ,cs_items
 ,ws_items
 where ss_items.item_id=cs_items.item_id
   and ss_items.item_id=ws_items.item_id
   and ss_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
   and ss_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
   and cs_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
   and cs_item_rev between 0.9 * ws_item_rev and 1.1 * ws_item_rev
   and ws_item_rev between 0.9 * ss_item_rev and 1.1 * ss_item_rev
   and ws_item_rev between 0.9 * cs_item_rev and 1.1 * cs_item_rev
 order by item_id,ss_item_rev
limit 100 SETTINGS distributed_product_mode = 'global', partial_merge_join_optimizations = 1, max_bytes_before_external_group_by = 50000000000, max_bytes_before_external_sort = 50000000000;
