--This is the query for fetching count of
--the new records by the first sponsor/collbarators.
select count(1), leadsponsorname
from clinic_trials
where to_date(lastupdateposted, 'Month DD, yyyy')>current_date-4
group by leadsponsorname
order by 1 desc;