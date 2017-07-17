# 建表

> 将下面代码保存为一个文件，用impala-shell -f filename 去执行，创建24张表

```
create database if not exists kudu_spark_tpcds_1000;
use kudu_spark_tpcds_1000;

drop table if exists call_center;

create table call_center(
      cc_call_center_sk         bigint               
,     cc_call_center_id         string              
,     cc_rec_start_date        string                         
,     cc_rec_end_date          string                         
,     cc_closed_date_sk         bigint                       
,     cc_open_date_sk           bigint                       
,     cc_name                   string                   
,     cc_class                  string                   
,     cc_employees              int                       
,     cc_sq_ft                  int                       
,     cc_hours                  string                      
,     cc_manager                string                   
,     cc_mkt_id                 int                       
,     cc_mkt_class              string                      
,     cc_mkt_desc               string                  
,     cc_market_manager         string                   
,     cc_division               int                       
,     cc_division_name          string                   
,     cc_company                int                       
,     cc_company_name           string                      
,     cc_street_number          string                      
,     cc_street_name            string                   
,     cc_street_type            string                      
,     cc_suite_number           string                      
,     cc_city                   string                   
,     cc_county                 string                   
,     cc_state                  string                       
,     cc_zip                    string                      
,     cc_country                string                   
,     cc_gmt_offset             double                  
,     cc_tax_percentage         double
,	PRIMARY KEY(cc_call_center_sk)
)
PARTITION BY HASH PARTITIONS 2
STORED AS KUDU;

create database if not exists kudu_spark_tpcds_1000;
use kudu_spark_tpcds_1000;

drop table if exists catalog_page;

create table catalog_page(
      cp_catalog_page_sk        bigint               
,     cp_catalog_page_id        string              
,     cp_start_date_sk          bigint                       
,     cp_end_date_sk            bigint                       
,     cp_department             string                   
,     cp_catalog_number         int                       
,     cp_catalog_page_number    int                       
,     cp_description            string                  
,     cp_type                   string
,	PRIMARY KEY(cp_catalog_page_sk)
)
PARTITION BY HASH PARTITIONS 2
STORED AS KUDU;


create database if not exists kudu_spark_tpcds_1000;
use kudu_spark_tpcds_1000;

drop table if exists catalog_returns;

create table catalog_returns
(
    cr_item_sk                bigint,
	cr_order_number           bigint,
    cr_returned_date_sk       bigint,
    cr_returned_time_sk       bigint,
    cr_refunded_customer_sk   bigint,
    cr_refunded_cdemo_sk      bigint,
    cr_refunded_hdemo_sk      bigint,
    cr_refunded_addr_sk       bigint,
    cr_returning_customer_sk  bigint,
    cr_returning_cdemo_sk     bigint,
    cr_returning_hdemo_sk     bigint,
    cr_returning_addr_sk      bigint,
    cr_call_center_sk         bigint,
    cr_catalog_page_sk        bigint,
    cr_ship_mode_sk           bigint,
    cr_warehouse_sk           bigint,
    cr_reason_sk              bigint,
    cr_return_quantity        int,
    cr_return_amount          double,
    cr_return_tax             double,
    cr_return_amt_inc_tax     double,
    cr_fee                    double,
    cr_return_ship_cost       double,
    cr_refunded_cash          double,
    cr_reversed_charge        double,
    cr_store_credit           double,
    cr_net_loss               double
	,	PRIMARY KEY(cr_item_sk,cr_order_number)
)
PARTITION BY HASH (cr_item_sk) PARTITIONS 16
STORED AS KUDU;


create database if not exists kudu_spark_tpcds_1000;

use kudu_spark_tpcds_1000;

drop table if exists catalog_sales;

create table catalog_sales
(
    cs_item_sk                bigint,
    cs_order_number           bigint,
    cs_sold_date_sk           bigint,
    cs_sold_time_sk           bigint,
    cs_ship_date_sk           bigint,
    cs_bill_customer_sk       bigint,
    cs_bill_cdemo_sk          bigint,
    cs_bill_hdemo_sk          bigint,
    cs_bill_addr_sk           bigint,
    cs_ship_customer_sk       bigint,
    cs_ship_cdemo_sk          bigint,
    cs_ship_hdemo_sk          bigint,
    cs_ship_addr_sk           bigint,
    cs_call_center_sk         bigint,
    cs_catalog_page_sk        bigint,
    cs_ship_mode_sk           bigint,
    cs_warehouse_sk           bigint,
    cs_promo_sk               bigint,
    cs_quantity               int,
    cs_wholesale_cost         double,
    cs_list_price             double,
    cs_sales_price            double,
    cs_ext_discount_amt       double,
    cs_ext_sales_price        double,
    cs_ext_wholesale_cost     double,
    cs_ext_list_price         double,
    cs_ext_tax                double,
    cs_coupon_amt             double,
    cs_ext_ship_cost          double,
    cs_net_paid               double,
    cs_net_paid_inc_tax       double,
    cs_net_paid_inc_ship      double,
    cs_net_paid_inc_ship_tax  double,
    cs_net_profit             double
	,	PRIMARY KEY(cs_item_sk,cs_order_number)
)
PARTITION BY HASH (cs_item_sk) PARTITIONS 64
STORED AS KUDU;



create database if not exists kudu_spark_tpcds_1000;
use kudu_spark_tpcds_1000;

drop table if exists customer_address;

create table customer_address
(
    ca_address_sk             bigint,
    ca_address_id             string,
    ca_street_number          string,
    ca_street_name            string,
    ca_street_type            string,
    ca_suite_number           string,
    ca_city                   string,
    ca_county                 string,
    ca_state                  string,
    ca_zip                    string,
    ca_country                string,
    ca_gmt_offset             double,
    ca_location_type          string
	,	PRIMARY KEY(ca_address_sk)
)
PARTITION BY HASH (ca_address_sk) PARTITIONS 6
STORED AS KUDU;


create database if not exists kudu_spark_tpcds_1000;
use kudu_spark_tpcds_1000;

drop table if exists customer_demographics;

create table customer_demographics
(
    cd_demo_sk                bigint,
    cd_gender                 string,
    cd_marital_status         string,
    cd_education_status       string,
    cd_purchase_estimate      int,
    cd_credit_rating          string,
    cd_dep_count              int,
    cd_dep_employed_count     int,
    cd_dep_college_count      int 
	,	PRIMARY KEY(cd_demo_sk)
)
PARTITION BY HASH (cd_demo_sk) PARTITIONS 2
STORED AS KUDU;


create database if not exists kudu_spark_tpcds_1000;
use kudu_spark_tpcds_1000;

drop table if exists customer;

create table customer
(
    c_customer_sk             bigint,
    c_customer_id             string,
    c_current_cdemo_sk        bigint,
    c_current_hdemo_sk        bigint,
    c_current_addr_sk         bigint,
    c_first_shipto_date_sk    bigint,
    c_first_sales_date_sk     bigint,
    c_salutation              string,
    c_first_name              string,
    c_last_name               string,
    c_preferred_cust_flag     string,
    c_birth_day               int,
    c_birth_month             int,
    c_birth_year              int,
    c_birth_country           string,
    c_login                   string,
    c_email_address           string,
    c_last_review_date        string
	,	PRIMARY KEY(c_customer_sk)
)
PARTITION BY HASH (c_customer_sk) PARTITIONS 8
STORED AS KUDU;


create database if not exists kudu_spark_tpcds_1000;
use kudu_spark_tpcds_1000;

drop table if exists date_dim;

create table date_dim
(
    d_date_sk                 bigint,
    d_date_id                 string,
    d_date                    string,
    d_month_seq               int,
    d_week_seq                int,
    d_quarter_seq             int,
    d_year                    int,
    d_dow                     int,
    d_moy                     int,
    d_dom                     int,
    d_qoy                     int,
    d_fy_year                 int,
    d_fy_quarter_seq          int,
    d_fy_week_seq             int,
    d_day_name                string,
    d_quarter_name            string,
    d_holiday                 string,
    d_weekend                 string,
    d_following_holiday       string,
    d_first_dom               int,
    d_last_dom                int,
    d_same_day_ly             int,
    d_same_day_lq             int,
    d_current_day             string,
    d_current_week            string,
    d_current_month           string,
    d_current_quarter         string,
    d_current_year            string 
	,	PRIMARY KEY(d_date_sk)
)
PARTITION BY HASH (d_date_sk) PARTITIONS 2
STORED AS KUDU;


create database if not exists kudu_spark_tpcds_1000;
use kudu_spark_tpcds_1000;

drop table if exists household_demographics;

create table household_demographics
(
    hd_demo_sk                bigint,
    hd_income_band_sk         bigint,
    hd_buy_potential          string,
    hd_dep_count              int,
    hd_vehicle_count          int
	,	PRIMARY KEY(hd_demo_sk)
)
PARTITION BY HASH (hd_demo_sk) PARTITIONS 2
STORED AS KUDU;



create database if not exists kudu_spark_tpcds_1000;
use kudu_spark_tpcds_1000;

drop table if exists income_band;

create table income_band(
      ib_income_band_sk         bigint               
,     ib_lower_bound            int                       
,     ib_upper_bound            int
,	PRIMARY KEY(ib_income_band_sk)
)
PARTITION BY HASH (ib_income_band_sk) PARTITIONS 2
STORED AS KUDU;


create database if not exists kudu_spark_tpcds_1000;
use kudu_spark_tpcds_1000;

drop table if exists inventory;

create table inventory
(
    inv_date_sk			bigint,
    inv_item_sk			bigint,
    inv_warehouse_sk		bigint,
    inv_quantity_on_hand	int
,	PRIMARY KEY(inv_date_sk)
)
PARTITION BY HASH (inv_date_sk) PARTITIONS 12
STORED AS KUDU;


create database if not exists kudu_spark_tpcds_1000;
use kudu_spark_tpcds_1000;

drop table if exists item;

create table item
(
    i_item_sk                 bigint,
    i_item_id                 string,
    i_rec_start_date          string,
    i_rec_end_date            string,
    i_item_desc               string,
    i_current_price           double,
    i_wholesale_cost          double,
    i_brand_id                int,
    i_brand                   string,
    i_class_id                int,
    i_class                   string,
    i_category_id             int,
    i_category                string,
    i_manufact_id             int,
    i_manufact                string,
    i_size                    string,
    i_formulation             string,
    i_color                   string,
    i_units                   string,
    i_container               string,
    i_manager_id              int,
    i_product_name            string
,	PRIMARY KEY(i_item_sk)
)
PARTITION BY HASH (i_item_sk) PARTITIONS 4
STORED AS KUDU;



create database if not exists kudu_spark_tpcds_1000;
use kudu_spark_tpcds_1000;

drop table if exists promotion;

create table promotion
(
    p_promo_sk                bigint,
    p_promo_id                string,
    p_start_date_sk           bigint,
    p_end_date_sk             bigint,
    p_item_sk                 bigint,
    p_cost                    double,
    p_response_target         int,
    p_promo_name              string,
    p_channel_dmail           string,
    p_channel_email           string,
    p_channel_catalog         string,
    p_channel_tv              string,
    p_channel_radio           string,
    p_channel_press           string,
    p_channel_event           string,
    p_channel_demo            string,
    p_channel_details         string,
    p_purpose                 string,
    p_discount_active         string 
,	PRIMARY KEY(p_promo_sk)
)
PARTITION BY HASH (p_promo_sk) PARTITIONS 2
STORED AS KUDU;


create database if not exists kudu_spark_tpcds_1000;
use kudu_spark_tpcds_1000;

drop table if exists reason;

create table reason(
      r_reason_sk               bigint               
,     r_reason_id               string              
,     r_reason_desc             string                
,	PRIMARY KEY(r_reason_sk)
)
PARTITION BY HASH (r_reason_sk) PARTITIONS 2
STORED AS KUDU;


create database if not exists kudu_spark_tpcds_1000;
use kudu_spark_tpcds_1000;

drop table if exists ship_mode;

create table ship_mode(
      sm_ship_mode_sk           bigint               
,     sm_ship_mode_id           string              
,     sm_type                   string                      
,     sm_code                   string                      
,     sm_carrier                string                      
,     sm_contract               string                      
,	PRIMARY KEY(sm_ship_mode_sk)
)
PARTITION BY HASH (sm_ship_mode_sk) PARTITIONS 2
STORED AS KUDU;


create database if not exists kudu_spark_tpcds_1000;
use kudu_spark_tpcds_1000;

drop table if exists store_returns;

create table store_returns
(
    sr_item_sk                bigint,
    sr_returned_date_sk       bigint,
    sr_return_time_sk         bigint,
    sr_customer_sk            bigint,
    sr_cdemo_sk               bigint,
    sr_hdemo_sk               bigint,
    sr_addr_sk                bigint,
    sr_store_sk               bigint,
    sr_reason_sk              bigint,
    sr_ticket_number          bigint,
    sr_return_quantity        int,
    sr_return_amt             double,
    sr_return_tax             double,
    sr_return_amt_inc_tax     double,
    sr_fee                    double,
    sr_return_ship_cost       double,
    sr_refunded_cash          double,
    sr_reversed_charge        double,
    sr_store_credit           double,
    sr_net_loss               double,
	PRIMARY KEY(sr_item_sk)
)
PARTITION BY HASH PARTITIONS 32
STORED AS KUDU;


create database if not exists kudu_spark_tpcds_1000;
use kudu_spark_tpcds_1000;

drop table if exists store_sales;

create table store_sales
(
    ss_item_sk                bigint,
    ss_sold_date_sk           bigint,
    ss_sold_time_sk           bigint,
    ss_customer_sk            bigint,
    ss_cdemo_sk               bigint,
    ss_hdemo_sk               bigint,
    ss_addr_sk                bigint,
    ss_store_sk               bigint,
    ss_promo_sk               bigint,
    ss_ticket_number          bigint,
    ss_quantity               int,
    ss_wholesale_cost         double,
    ss_list_price             double,
    ss_sales_price            double,
    ss_ext_discount_amt       double,
    ss_ext_sales_price        double,
    ss_ext_wholesale_cost     double,
    ss_ext_list_price         double,
    ss_ext_tax                double,
    ss_coupon_amt             double,
    ss_net_paid               double,
    ss_net_paid_inc_tax       double,
    ss_net_profit             double                  
,	PRIMARY KEY(ss_item_sk)
)
PARTITION BY HASH (ss_item_sk) PARTITIONS 96
STORED AS KUDU;


create database if not exists kudu_spark_tpcds_1000;
use kudu_spark_tpcds_1000;

drop table if exists store;

create table store
(
    s_store_sk                bigint,
    s_store_id                string,
    s_rec_start_date          string,
    s_rec_end_date            string,
    s_closed_date_sk          bigint,
    s_store_name              string,
    s_number_employees        int,
    s_floor_space             int,
    s_hours                   string,
    s_manager                 string,
    s_market_id               int,
    s_geography_class         string,
    s_market_desc             string,
    s_market_manager          string,
    s_division_id             int,
    s_division_name           string,
    s_company_id              int,
    s_company_name            string,
    s_street_number           string,
    s_street_name             string,
    s_street_type             string,
    s_suite_number            string,
    s_city                    string,
    s_county                  string,
    s_state                   string,
    s_zip                     string,
    s_country                 string,
    s_gmt_offset              double,
    s_tax_precentage          double                  
,	PRIMARY KEY(s_store_sk)
)
PARTITION BY HASH (s_store_sk) PARTITIONS 2
STORED AS KUDU;


create database if not exists kudu_spark_tpcds_1000;
use kudu_spark_tpcds_1000;

drop table if exists time_dim;

create table time_dim
(
    t_time_sk                 bigint,
    t_time_id                 string,
    t_time                    int,
    t_hour                    int,
    t_minute                  int,
    t_second                  int,
    t_am_pm                   string,
    t_shift                   string,
    t_sub_shift               string,
    t_meal_time               string
,	PRIMARY KEY(t_time_sk)
)
PARTITION BY HASH (t_time_sk) PARTITIONS 2
STORED AS KUDU;


create database if not exists kudu_spark_tpcds_1000;
use kudu_spark_tpcds_1000;

drop table if exists warehouse;

create table warehouse(
      w_warehouse_sk            bigint               
,     w_warehouse_id            string              
,     w_warehouse_name          string                   
,     w_warehouse_sq_ft         int                       
,     w_street_number           string                      
,     w_street_name             string                   
,     w_street_type             string                      
,     w_suite_number            string                      
,     w_city                    string                   
,     w_county                  string                   
,     w_state                   string                       
,     w_zip                     string                      
,     w_country                 string                   
,     w_gmt_offset              double                  
,	PRIMARY KEY(w_warehouse_sk)
)
PARTITION BY HASH (w_warehouse_sk) PARTITIONS 2
STORED AS KUDU;


create database if not exists kudu_spark_tpcds_1000;
use kudu_spark_tpcds_1000;

drop table if exists web_page;

create table web_page(
      wp_web_page_sk            bigint               
,     wp_web_page_id            string              
,     wp_rec_start_date        string                         
,     wp_rec_end_date          string                         
,     wp_creation_date_sk       bigint                       
,     wp_access_date_sk         bigint                       
,     wp_autogen_flag           string                       
,     wp_customer_sk            bigint                       
,     wp_url                    string                  
,     wp_type                   string                      
,     wp_char_count             int                       
,     wp_link_count             int                       
,     wp_image_count            int                       
,     wp_max_ad_count           int
,	PRIMARY KEY(wp_web_page_sk)
)
PARTITION BY HASH (wp_web_page_sk) PARTITIONS 2
STORED AS KUDU;


create database if not exists kudu_spark_tpcds_1000;
use kudu_spark_tpcds_1000;

drop table if exists web_returns;

create table web_returns
(
    wr_item_sk                bigint,
    wr_returned_date_sk       bigint,
    wr_returned_time_sk       bigint,
    wr_refunded_customer_sk   bigint,
    wr_refunded_cdemo_sk      bigint,
    wr_refunded_hdemo_sk      bigint,
    wr_refunded_addr_sk       bigint,
    wr_returning_customer_sk  bigint,
    wr_returning_cdemo_sk     bigint,
    wr_returning_hdemo_sk     bigint,
    wr_returning_addr_sk      bigint,
    wr_web_page_sk            bigint,
    wr_reason_sk              bigint,
    wr_order_number           bigint,
    wr_return_quantity        int,
    wr_return_amt             double,
    wr_return_tax             double,
    wr_return_amt_inc_tax     double,
    wr_fee                    double,
    wr_return_ship_cost       double,
    wr_refunded_cash          double,
    wr_reversed_charge        double,
    wr_account_credit         double,
    wr_net_loss               double
,	PRIMARY KEY(wr_item_sk)
)
PARTITION BY HASH (wr_item_sk) PARTITIONS 8
STORED AS KUDU;


create database if not exists kudu_spark_tpcds_1000;
use kudu_spark_tpcds_1000;

drop table if exists web_sales;

create table web_sales
(
    ws_item_sk                bigint,
    ws_sold_date_sk           bigint,
    ws_sold_time_sk           bigint,
    ws_ship_date_sk           bigint,
    ws_bill_customer_sk       bigint,
    ws_bill_cdemo_sk          bigint,
    ws_bill_hdemo_sk          bigint,
    ws_bill_addr_sk           bigint,
    ws_ship_customer_sk       bigint,
    ws_ship_cdemo_sk          bigint,
    ws_ship_hdemo_sk          bigint,
    ws_ship_addr_sk           bigint,
    ws_web_page_sk            bigint,
    ws_web_site_sk            bigint,
    ws_ship_mode_sk           bigint,
    ws_warehouse_sk           bigint,
    ws_promo_sk               bigint,
    ws_order_number           bigint,
    ws_quantity               int,
    ws_wholesale_cost         double,
    ws_list_price             double,
    ws_sales_price            double,
    ws_ext_discount_amt       double,
    ws_ext_sales_price        double,
    ws_ext_wholesale_cost     double,
    ws_ext_list_price         double,
    ws_ext_tax                double,
    ws_coupon_amt             double,
    ws_ext_ship_cost          double,
    ws_net_paid               double,
    ws_net_paid_inc_tax       double,
    ws_net_paid_inc_ship      double,
    ws_net_paid_inc_ship_tax  double,
    ws_net_profit             double
,	PRIMARY KEY(ws_item_sk)
)
PARTITION BY HASH (ws_item_sk) PARTITIONS 64
STORED AS KUDU;


create database if not exists kudu_spark_tpcds_1000;
use kudu_spark_tpcds_1000;

drop table if exists web_site;

create table web_site
(
    web_site_sk           bigint,
    web_site_id           string,
    web_rec_start_date    string,
    web_rec_end_date      string,
    web_name              string,
    web_open_date_sk      bigint,
    web_close_date_sk     bigint,
    web_class             string,
    web_manager           string,
    web_mkt_id            int,
    web_mkt_class         string,
    web_mkt_desc          string,
    web_market_manager    string,
    web_company_id        int,
    web_company_name      string,
    web_street_number     string,
    web_street_name       string,
    web_street_type       string,
    web_suite_number      string,
    web_city              string,
    web_county            string,
    web_state             string,
    web_zip               string,
    web_country           string,
    web_gmt_offset        double,
    web_tax_percentage    double
,	PRIMARY KEY(web_site_sk)
)
PARTITION BY HASH (web_site_sk) PARTITIONS 2
STORED AS KUDU;



```

> 报错1

如果出现如下报错：
ERROR:
ImpalaRuntimeException: Error creating Kudu table 'impala::kudu_spark_tpcds_1000.catalog_sales'
CAUSED BY: NonRecoverableException: The requested number of tablets is over the permitted maximum (100)

貌似机器数量限制，我这里5台机器，貌似每台机器没个表最大20个分区，单个hash分区最大分区数是100，多个hash分区则相乘最大100

> 报错2

ERROR:
ImpalaRuntimeException: Error creating Kudu table 'impala::default.web_site'
CAUSED BY: NonRecoverableException: Got out-of-order key column: name: "web_site_sk" type: INT64 is_key: true is_nullable: false cfile_block_size: 0

这是因为hash分区的字段没在排序首位，放到首位就好了，这是一个坑，顺序对不上导入数据时会造成很多麻烦


# 数据导入

> 1，这里直接从impala导入，也可以看首页README通过spark导入，但是顺序对不上，所以放弃这种方式

```
for i in call_center            	catalog_page           	catalog_returns        	catalog_sales          	customer               	customer_address  	customer_demographics  	date_dim               	household_demographics 	income_band            	inventory              	item                   	promotion              	reason                 	ship_mode              	store                  	store_returns          	store_sales            	time_dim          	warehouse              	web_page               	web_returns            	web_sales              	web_site

do
	echo $i================================
	impala-shell -q "INSERT INTO $i SELECT * FROM tpcds_bin_partitioned_parquet_1000.$i" --quiet
done
```

> 2，采用spark导入

```
详细代码见src目录，更改main函数的相应数据库表名就可以直接大jar包通过spark-submit提交使用了

```