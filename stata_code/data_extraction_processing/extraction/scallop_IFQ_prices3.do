#delimit;
clear;
set more off;
pause off;




/* Lease extractions 
The form asks for "PRICE RECEIVED FOR THE TRANSFER"
*/
clear;
odbc load,  exec("select mq.auth_type as to_auth_type, mq.per_num as mq_to_permit, ga.lease_exch_id, ga.from_permit, ga.to_permit, ga.from_right, ga.to_right, ga.approval_date, ga.fishing_year, ga.quantity, ga.price, ga.remark 
  from ams.lease_exch_applic@garfo_nefsc  ga, mqrs.mort_elig_criteria@garfo_nefsc mq
	where ga.FMP='SCAG' and ga.approval_status='APPROVED'
  and mq.right_id=ga.to_right and
    trunc(ga.approval_date)>=(mq.date_eligible)
      and (trunc(ga.approval_date)<=mq.date_cancelled OR mq.date_cancelled is null)
  and mq.fishery='GENERAL CATEGORY SCALLOP' and mq.DATE_ELIGIBLE is NOT NULL ;") $oracle_cxn clear;
rename price value;
gen price=value/quantity;

save "${data_main}/approved_leases_lp_$today_date_string.dta", replace;



clear;

odbc load,  exec("select mq.auth_type as from_auth_type, mq.per_num as mq_from_permit, ga.lease_exch_id 
  from ams.lease_exch_applic@garfo_nefsc  ga, mqrs.mort_elig_criteria@garfo_nefsc mq
	where ga.FMP='SCAG' and ga.approval_status='APPROVED'
  and mq.right_id=ga.from_right and
    trunc(ga.approval_date)>=(mq.date_eligible)
      and (trunc(ga.approval_date)<=mq.date_cancelled OR mq.date_cancelled is null)
  and mq.fishery='GENERAL CATEGORY SCALLOP' and mq.DATE_ELIGIBLE is NOT NULL ;") $oracle_cxn  clear;
merge 1:1 lease_exch_id using "${data_main}/approved_leases_lp_$today_date_string.dta";

/*****************************************/
notes: this file was made by "lease_extractions_v3_lp.do" on $today_date_string;
/*****************************************/

drop from_permit to_permit;
rename mq_to_permit to_permit;
rename mq_from_permit from_permit;
gen adate=dofc(approval_date);
drop approval_date;
rename adate approval_date;
format approval_date %td;
drop _merge;
save "$data_main/approved_leases_lp_$today_date_string.dta", replace;

/* append in ap_nums and business data */

order fishing_year;
order from_permit from_right to_permit to_right quantity value,after(approval);

save "$data_main/approved_leases_lp_$today_date_string.dta",replace;

keep approval_date  quantity value price to_permit from_permit;
gen approval_year=yofd(approval_date);
gen approval_month=month(approval_date);


drop if price>10;

drop if value==1;
drop if value==0;
drop if price<=0.20;
collapse (sum) quantity value, by(approval_year approval_month to_permit from_permit);
egen tfrom=tag(approval_month approval_year from_permit);
egen tto=tag(approval_month approval_year to_permit);

collapse (sum) quantity value tfrom tto, by(approval_year approval_month);


gen cal_month=ym(approval_year, approval_month);

tsset cal_month;

format cal_month %tm;
gen scallop_fy=approval_year;
replace scallop_fy=scallop_fy-1 if approval_month<=2;
gen price=value/quantity;

drop approval*;

order cal_month;
rename tfrom number_of_sellers;
rename tto number_of_buyers;
save "$data_main/scallop_IFQ_prices_$today_date_string.dta", replace ;

