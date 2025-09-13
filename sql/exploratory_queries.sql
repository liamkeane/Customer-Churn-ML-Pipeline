use telco;

select
	c.customer_id,
	c.tenure_months,
	d.gender,
	d.age
from customer_profile c
inner join demographics d on c.customer_id = d.customer_id;
	