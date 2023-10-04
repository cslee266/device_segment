drop table if exists device_segment;
create table device_segment as
with records as (
select billing_subscriber_id, device_brand, device_model, substr(device_imei,1,8) device_tac,
case when device_brand in ('APPLE','SAMSUNG','OPPO','HUAWEI','VIVO','XIAOMI','REALME','ASUS',
'LENOVO','SONY','ONEPLUS','HTC','NOKIA','GOOGLE','MOTOROLA','HONOR') then device_brand
     when device_brand in ('HMD GLOBAL') then 'NOKIA'
     when device_brand in ('BBK') then 'VIVO'
     when device_type in ('SMART PHONE','SMART_PHONE') then 'OTHERS'
     when device_type in ('FEATURE PHONE','FEATURE_PHONE','BASIC_PHONE') then 'FEATURE_PHONE'
     when device_type in ('TABLETS') then 'TABLET'
     when device_type in ('DONGLE') then device_type
end as brand_grouping,
last_device_change_date,
-- detecting change in device brand or device model
case when nvl(lag(device_brand) over(partition by billing_subscriber_id order by day_key),device_brand)!=device_brand then 1 else 0 end as brand_change,
case when nvl(lag(device_model) over(partition by billing_subscriber_id order by day_key),device_model)!=device_model then 1 else 0 end as model_change,
substr(day_key,1,6) month_key
from profile_tbl
where (substr(day_key,7,2) = '01'
and subscriber_count = 1
and service_type = 'POSTPAID'
and line_of_business = 'MOBILE'
and account_category = '10-CONSUMER'
and device_brand != 'UNKNOWN'
),

-- calculating what is the average length of usage (months) for each model
model_change as (
select x.*,
avg(device_usage_months) over(partition by billing_subscriber_id) avg_device_usage_months
from (
    select billing_subscriber_id, brand_grouping, device_brand, device_model,
    count(distinct last_device_change_date) interchange_count,
    min(last_device_change_date) device_start_date,
    count(month_key) device_usage_months
    from records
    group by 1,2,3,4
    ) x
),

-- calculating for each customer, what is the dominant/preferred device brand and its weightage
brand_change as (
select billing_subscriber_id, avg_device_usage_months, brand_grouping, device_brand, brand_usage_months, 
first_value(brand_grouping) over(partition by billing_subscriber_id order by brand_usage_months desc) dominant_brand,
brand_usage_months/sum(brand_usage_months) over(partition by billing_subscriber_id) dominant_brand_wt
from (
    -- roll up from model change to brand change
    select billing_subscriber_id, avg_device_usage_months, brand_grouping, device_brand,
    sum(device_usage_months) brand_usage_months
    from model_change
    group by 1,2,3,4
    ) x
),

-- some brand statistics for each subscriber
brand_stats as (
select billing_subscriber_id, 
group_concat(distinct brand_grouping,'-') brand,
max(dominant_brand) dominant_brand,
max(dominant_brand_wt) dominant_brand_wt,
max(brand_usage_months) dominant_brand_mths,
count(distinct brand_grouping) count_brand,
avg_device_usage_months
from brand_change
group by 1,7
),

-- device order records
device_match as (
select distinct billing_subscriber_id, device_name, substr(imei,1,8) tac,
from_timestamp(
case when day(device_active_date) = 1 then device_active_date
     when day(device_active_date) > 1 then add_months(device_active_date,1)
end,'yyyyMM') as month_key
from device_order_tbl
where partitioncolumn >= '201808'
),

-- check if device is bought from company
device_purchase as (
select billing_subscriber_id, 
max(device_purchase) device_purchase,
max(case when device_purchase=1 then device_name end) last_purchase_model, 
max(case when device_purchase=1 then tac end) last_purchase_tac, 
max(case when device_purchase=1 then month_key end) last_purchase_date
from (
    select *, row_number() over(partition by billing_subscriber_id order by month_key desc) device_purchase
    from device_match
    ) x
group by 1
),

device_change as (
select z.billing_subscriber_id, device_brand, device_model, device_tac, brand_grouping, z.month_key, brand_number, model_number,
c.device_name purchased_device,
case when c.billing_subscriber_id is not null then 1 else 0 end as device_match,
d.device_purchase,
d.last_purchase_model,
d.last_purchase_tac,
d.last_purchase_date
from (
    select billing_subscriber_id, device_brand, device_model, device_tac, brand_grouping, y.month_key,
    dense_rank() over(partition by billing_subscriber_id order by brand_number desc) brand_number, --latest brand first, older brands come later
    dense_rank() over(partition by billing_subscriber_id order by model_number desc) model_number  --latest model first, older models come later
    from (
        select a.billing_subscriber_id, device_brand, device_model, device_tac, brand_grouping, a.month_key, brand_change,
        sum(brand_change) over(
            partition by a.billing_subscriber_id
            order by a.month_key
            rows between unbounded preceding and current row --cumsum to get number of brands changed
            ) as brand_number, b.model_number
        from records a
        left join (
            select billing_subscriber_id, month_key,
            sum(model_change) over(
                partition by billing_subscriber_id
                order by month_key
                rows between unbounded preceding and current row --cumsum to get number of models changed
                ) as model_number
            from records
            ) b
        on a.billing_subscriber_id=b.billing_subscriber_id and a.month_key=b.month_key
        ) y
    ) z
    left join device_match c
    on z.billing_subscriber_id=c.billing_subscriber_id and z.device_tac=c.tac and z.month_key=c.month_key
    left join device_purchase d
    on z.billing_subscriber_id=d.billing_subscriber_id
),

-- stats on device change and usage
device_stats as (
select billing_subscriber_id,
max(brand_number) brand_change_count,
max(model_number) model_change_count,
max(case when brand_number=1 then device_brand end) latest_brand,
max(case when model_number=1 then device_model end) latest_model,
max(case when model_number=1 then device_tac end) latest_tac,
count(case when brand_number=1 then 1 end) latest_brand_streak,
count(case when model_number=1 then 1 end) latest_model_streak,
count(distinct case when brand_number=1 then device_model end) model_count_latest_brand,
sum(device_match) device_match,
max(device_purchase) device_purchase,
max(last_purchase_model) last_purchase_model,
max(last_purchase_tac) last_purchase_tac,
max(last_purchase_date) last_purchase_date
from device_change
group by 1
),

-- scraped data from GSMArena
device_info as (
select profile_tac_number tac, 
device_brand, 
profile_device_model device_model, 
scraped_device_name device_name,
substr(device_launch_date,1,6) device_launch,
rrp_price_tiering device_tier
from gsmarena_data
),

-- get earliest date for each TAC (type allocation code)
device_min_date as (
select device_tac, min(from_timestamp(last_device_change_date,'yyyyMM')) min_date
from records
group by 1
),

final as (
select a.billing_subscriber_id,
--distinct brands used throughout the customer's tenure with company, e.g. "APPLE-SAMSUNG","VIVO-OPPO"
btrim(regexp_replace(
        concat_ws("-",
          regexp_extract(brand,"(APPLE)",1),
          regexp_extract(brand,"(SAMSUNG)",1),
          regexp_extract(brand,"(OPPO)",1),
          regexp_extract(brand,"(HUAWEI)",1),
          regexp_extract(brand,"(VIVO)",1),
          regexp_extract(brand,"(XIAOMI)",1),
          regexp_extract(brand,"(REALME)",1),
          regexp_extract(brand,"(ASUS)",1),
          regexp_extract(brand,"(LENOVO)",1),
          regexp_extract(brand,"(SONY)",1),
          regexp_extract(brand,"(ONEPLUS)",1),
          regexp_extract(brand,"(HTC)",1),
          regexp_extract(brand,"(NOKIA)",1),
          regexp_extract(brand,"(GOOGLE)",1),
          regexp_extract(brand,"(MOTOROLA)",1),
          regexp_extract(brand,"(HONOR)",1),
          regexp_extract(brand,"(OTHERS)",1),
          regexp_extract(brand,"(FEATURE_PHONE)",1),
          regexp_extract(brand,"(TABLET)",1),
          regexp_extract(brand,"(DONGLE)",1)
        ),"([[:word:]])-+","\\1-"),"-") as brand, 
b.latest_tac,
b.latest_brand,
b.latest_model,
c.device_name latest_device_name,
nvl(c.device_launch,d.min_date) device_launch,
c.device_tier,
a.dominant_brand,
a.dominant_brand_wt,
a.dominant_brand_mths,
a.count_brand,
a.avg_device_usage_months,
b.brand_change_count,
b.model_change_count,
b.latest_brand_streak,
b.latest_model_streak,
b.model_count_latest_brand,
b.device_match,
b.device_purchase,
b.last_purchase_model,
b.last_purchase_tac,
b.last_purchase_date,
case when b.latest_tac=b.last_purchase_tac then 1 else 0 end as is_latest_device_c
from brand_stats a
left join device_stats b
on a.billing_subscriber_id=b.billing_subscriber_id
left join device_info c
on b.latest_tac=c.tac
left join device_min_date d
on b.latest_tac=d.device_tac
)

select *
from final
;


-- create customer segments
select *, 
datediff(now(),to_timestamp(device_launch,'yyyyMMdd'))/30 - latest_model_streak  device_launch_duration,
case when latest_brand=dominant_brand then 'LOYALIST'
	 else 'EXPLORER'
end as brand_affinity,

case when device_launch_duration - latest_model_streak <= 3 then 'HUNTER'
	 when device_launch_duration - latest_model_streak > 3 and device_launch_duration - latest_model_streak <= 12 then 'OBSERVER'
	 when device_launch_duration - latest_model_streak > 12 then 'RESEARCHER'
end as device_purchase_speed,

case when price_tiering = 'HIGH' then 'DEEP POCKETS'
	 when price_tiering = 'HIGH' AND 'MID' then 'MIXED'
	 when price_tiering = 'LOW' then 'DEAL SEEKER'
end as purchasing_power

from (
	select *,
	months_between(now(),to_timestamp(device_launch,'yyyyMMdd')) device_launch_duration
	from device_segment_tbl
	) x
;