select
	c.customer_id,
	c.tenure_months,
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
    f.total_refunds,
    f.total_extra_data_charges,
    f.total_long_distance_charges,
    f.total_revenue,
    se.has_referred_a_friend,
    se.number_of_referrals,
    se.has_phone_service,
    se.avg_monthly_long_distance_charges,
    se.has_internet_service,
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
    st.churn_score
from customer_profile c
left join demographics d on c.customer_id = d.customer_id
left join location l on c.customer_id = l.customer_id
inner join population p on l.zip_code = p.zip_code
left join financials f on c.customer_id = f.customer_id
left join services se on c.customer_id = se.customer_id
left join status st on c.customer_id = st.customer_id;
