with clks as 
(
select distinct(clickstream.userid) as "User_ID",
       clickstream.timestampist::date as "Date",
           clickstream.eventlabel as "Event_Label",
           clickstream.eventcategory as "Event_Category",
           v1 as "value1",
           v2 as "value2"
           
    from clickstream
    
    where {{Date1}}
    and clickstream.eventlabel = 'Payment_Status'
    and (v1 is not null or v1 not ilike '')
    and v2 = 'Success'
    order by 5
)

select      clickstream.timestampist::date as "Date",

            count(distinct(case when  clickstream.apptrackingid in ('1.3.4','1.3.5','1.3.6','1.3.7','1.3.8', '1.3.9', '1.4.0', '1.4.1', '1.4.2', '1.4.3', '1.4.4', '1.4.5', '1.4.6', '1.4.7', '1.4.8', '1.4.9', '1.5.0', '1.5.1', '1.5.2', '1.5.3', '1.5.4', '1.5.5', '1.5.3rrr', '1.5.6', '1.5.7', '1.5.8', '1.5.9', '1.6.0', '1.6.1', '1.6.2', '1.6.3', '1.6.4', '1.6.5', '1.6.6', '1.6.7') then clickstream.userid END)) as "DAU",
            
            count(distinct(case when  clickstream.apptrackingid in ('1.3.4','1.3.5','1.3.6','1.3.7','1.3.8', '1.3.9', '1.4.0', '1.4.1', '1.4.2', '1.4.3', '1.4.4', '1.4.5', '1.4.6', '1.4.7', '1.4.8', '1.4.9', '1.5.0', '1.5.1', '1.5.2', '1.5.3', '1.5.4', '1.5.5', '1.5.3rrr', '1.5.6', '1.5.7', '1.5.8', '1.5.9', '1.6.0', '1.6.1', '1.6.2', '1.6.3', '1.6.4', '1.6.5', '1.6.6', '1.6.7') and (clickstream.eventlabel ilike 'app_screen_open' and v1 ilike 'courses') then clickstream.userid END)) as "Course Page Landing",
            count(distinct(case when  clickstream.apptrackingid in ('1.3.4','1.3.5','1.3.6','1.3.7','1.3.8', '1.3.9', '1.4.0', '1.4.1', '1.4.2', '1.4.3', '1.4.4', '1.4.5', '1.4.6', '1.4.7', '1.4.8', '1.4.9', '1.5.0', '1.5.1', '1.5.2', '1.5.3', '1.5.4', '1.5.5', '1.5.3rrr', '1.5.6', '1.5.7', '1.5.8', '1.5.9', '1.6.0', '1.6.1', '1.6.2', '1.6.3', '1.6.4', '1.6.5', '1.6.6', '1.6.7') and clickstream.eventlabel in ('app_course_search_result_click','app_course_listing_click','app_course_collection_course_click') then clickstream.userid END)) as "Course Click",
            
            count(distinct(case when clickstream.eventlabel = 'app_course_buy_click'  and clickstream.apptrackingid in ('1.3.4','1.3.5','1.3.6','1.3.7','1.3.8', '1.3.9', '1.4.0', '1.4.1', '1.4.2', '1.4.3', '1.4.4', '1.4.5', '1.4.6', '1.4.7', '1.4.8', '1.4.9', '1.5.0', '1.5.1', '1.5.2', '1.5.3', '1.5.4', '1.5.5', '1.5.3rrr', '1.5.6', '1.5.7', '1.5.8', '1.5.9', '1.6.0', '1.6.1', '1.6.2', '1.6.3', '1.6.4', '1.6.5', '1.6.6', '1.6.7')  then clickstream.userid end)) as "Course Buy Now Click" ,
            
            count(distinct(case when clickstream.eventlabel = 'Payments_Landing' then clickstream.userid end)) as "Payments Landing",
            
            count(distinct(case when clickstream.eventlabel in ('Payment_Instrument_Select','Vedantu_Wallet_Select') then clickstream.userid end)) as "Instrument Selection",
            
            count(case when clickstream.eventlabel = 'Pay_Now' then clickstream.userid end) as "Pay Now Click",
            
            count(case when (clickstream.eventcategory='growth_app' AND clickstream.eventlabel='app_payment_Status'  and v2 = 'Success') then clickstream.userid end) as "Payment Success (App)",
            
            count(case when (clickstream.eventlabel in ('Payment_Status','app_payment_Status')  and v2 = 'Failure' and clickstream.eventcategory ilike 'growth_app') or clickstream.eventlabel = 'app_payment_failure' then clickstream.userid end) as "Payment Failed (App)",
            
            count(case when clickstream.eventlabel = 'Payment_Status_Success'  then clickstream.userid end) as "Payment Success (Web)",
            
            count(case when clickstream.eventlabel = 'Payment_Status_Failure'  then clickstream.userid end) as "Payment Failed (Web)"
            
            
            from clickstream
                 left outer join clks on clks.User_ID = clickstream.userid

            where {{Date1}}
            
            group by 1
            
            order by 1 desc
