-- select * from pysparkdbt.bronze.trips

select * from  {{source('source_bronze','trips')}}