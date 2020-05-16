select      clickstream.timestampist::date as "Date",

            count(case when clickstream.eventlabel = 'Pay_Now' then clickstream.userid end) as "Pay Now Click",

            count(case when clickstream.eventlabel = 'Pay_Now' and v2 = 'PAYPAL'  then clickstream.userid end) as "PayPal selection Click",
            
            count(case when (clickstream.eventlabel = 'app_payment_Status'  and v2 = 'Success' and clickstream.eventcategory ilike 'growth_app') or clickstream.eventlabel ilike 'app_payment_success' then paypal_data.user_id end) as "Payment Success (App)",
            
            count(case when (clickstream.eventlabel = 'app_payment_Status'  and v2 = 'Failure' and clickstream.eventcategory ilike 'growth_app') or clickstream.eventlabel ilike 'app_payment_failure' then paypal_data.user_id end) as "Payment Failed (App)",
            
            count(case when clickstream.eventlabel = 'Payment_Status_Success'  then paypal_data.user_id end) as "Payment Success (Web)",
            
            count(case when clickstream.eventlabel = 'Payment_Status_Failure'  then paypal_data.user_id end) as "Payment Failed (Web)"
            
            
            from clickstream
                 left join (select distinct(case when clickstream.eventlabel = 'Pay_Now' and v2 = 'PAYPAL'  then clickstream.userid end) as user_id from clickstream where {{Date1}}) as paypal_data on paypal_data.user_id = clickstream.userid

            where {{Date1}}
            
            group by 1
            
            order by 1 desc
