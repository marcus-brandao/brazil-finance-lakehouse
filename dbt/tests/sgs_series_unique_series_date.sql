-- One row per series and date. Returns the pairs that appear more than once.
select series_code, date, count(*) as n
from {{ ref('sgs_series') }}
group by series_code, date
having count(*) > 1
