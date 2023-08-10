#delimit;

clear;

	odbc load,  exec("select * from vps_vessel where ap_year>=2004 and vp_num in (
	select distinct vp_num from vps_vessel where ap_num in (
		select distinct ap_num from permit.vps_owner where business_id in 
			(select distinct business_id  from permit.bus_own where name_last like '%Rafael%' and (name_first like '%Carlos%' OR name_first like '%Conceicao%')
			)
		)
	)
order by vp_num, ap_num
;") $oracle_cxn;

saveold $my_workdir/carlos_ever_$today_date_string.dta, replace version(12);

clear;




	odbc load,  exec("
select * from vps_vessel where ap_num in (
	select distinct ap_num from permit.vps_owner where business_id in 
		(select distinct business_id  from permit.bus_own where name_last like '%Rafael%' and (name_first like '%Carlos%' OR name_first like '%Conceicao%')
		)
	)
order by vp_num, ap_num
;") $oracle_cxn;

saveold $my_workdir/carlos_owns_these_$today_date_string.dta, replace version(12);
