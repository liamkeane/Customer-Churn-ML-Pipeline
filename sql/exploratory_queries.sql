use telco;

select
	c.customer_id,
	c.tenure_months,
	d.gender,
	d.age,
	d.is_under_30,
    d.is_senior_citizen,
    d.has_partner,
    d.has_dependents,
    d.number_of_dependents,
    l.city,
    l.zip_code,
    p.population,
    f.contract_type,
    f.has_paperless_billing,
    f.payment_method,
    f.monthly_charges,
    f.total_charges,
    f.total_refunds,
    f.total_extra_data_charges,
    f.total_long_distance_charges,
    f.total_revenue,
    se.has_referred_a_friend,
    se.number_of_referrals,
    se.has_phone_service,
    se.avg_monthly_long_distance_charges,
    se.has_multiple_lines,
    se.has_internet_service,
    se.internet_service_type,
    se.avg_monthly_gb_download,
    se.has_online_security,
    se.has_online_backup,
    se.has_device_protection,
    se.has_tech_support,
    se.has_tv,
    se.has_movies,
    se.has_music,
    se.has_unlimited_data,
    st.satisfaction_score,
    st.churn_label,
    st.churn_score,
    st.cltv,
    st.churn_category
from customer_profile c
inner join demographics d on c.customer_id = d.customer_id
inner join location l on c.customer_id = l.customer_id
inner join population p on l.zip_code = p.zip_code
inner join financials f on c.customer_id = f.customer_id
inner join services se on c.customer_id = se.customer_id
inner join status st on c.customer_id = st.customer_id;






