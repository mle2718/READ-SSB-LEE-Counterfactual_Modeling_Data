

/*

	DAS (2004/5)
	DAS2 (2006-8)
	AMS (2009-2017)



For 2009 to present,
	AMS.TRIP_AND_CHARGE has permit_nbr, datesail and dateland and the charge.
		Charge type is T, C, S.
			1 'C' entry and it's negative. (Compensation?)
			4 'S' entries.  All from FY 2016. MULT_MONK_USAGE.
		Running clock is Y/N

		AMS.lease_exch_applic
 */

#delimit;


*quietly do "/home/mlee/Documents/Workspace/technical folder/do file scraps/odbc_connection_macros.do";
*global oracle_cxn "conn("$mysole_conn") lower";
local date: display %td_CCYY_NN_DD date(c(current_date), "DMY");
global today_date_string = subinstr(trim("`date'"), " " , "_", .);
clear;

tempfile das2_usage das1_usage reg_use2006 das2a initial vaf;


clear;
odbc load,  exec("select * from das2.valid_accounting_function")  $oracle_cxn;  
drop calendar parameter;
drop if start_date>=end_date;
replace end_date=end_date+msofhours(24)-1;
save `vaf', replace;


use  $my_workdir/das2_activity_codes.dta, clear;

merge 1:m activity_code using `vaf';
drop if _merge==1;
assert _merge==3;
drop _merge;
keep if plan=="MUL";

save  $my_workdir/das2_activity_codes_augmented.dta, replace;




clear;
/* DAS schema data*/
/* usage */

odbc load,  exec("select du.fishing_year, du.das_transaction_id, du.permit_number, du.das_charged_days, tr.sailing_port, tr.sailing_state, tr.sail_date_time as date_sail, tr.landing_port, tr.landing_state, tr.landing_date_time as date_land,
		tr.gillnet_vessel, tr.day_trip, tr.observer_onboard, tr.das_charged_fixed, tr.fishery_code, tr.vessel_name, du.trip_length_days 		
	from das.das_used du, das.trips tr where du.das_transaction_id=tr.das_transaction_id and du.permit_number=tr.permit_number and du.das_category='A' and du.fishery='MUL';") $oracle_cxn;  
	keep if inlist(fishing_year, 2004, 2005);
rename permit permit;
rename das_charged_days charge;
rename fishery_code activity_code;

collapse (sum) charge trip_length_days (first) gillnet_vessel day_trip observer_onboard das_charged_fixed vessel_name , by(das_transaction_id fishing_year permit sailing_port sailing_state date_sail landing_port landing_state date_land activity_code );
gen schema="DAS";

save `das1_usage',replace;

/* das_transaction_id has no duplicates here.*/


clear;









/*DAS2 schema DAS2.allocation_use
 A. Trips */
odbc load,  exec(" select du.das_trip_id, du.allocation_use_type, du.au_date_time_debited, du.permit_debited, du.permit_credited, du.quantity, du.category_name, du.plan, du.right_id, du.credit_type, 
du.fishing_year, du.dollar_value, activity_code, dt.permit_num, dt.sailing_port, dt.sailing_state, dt.trip_start, dt.trip_end, dt.landing_port, dt.landing_state,nvl(dt.external_charge,0) as external_charge
 from das2.allocation_use du, das2.das_trip dt
 where du.category_name='A' and du.plan='MUL' and du.allocation_use_type='TRIP' and du.quantity<>0
 and du.das_trip_id=dt.das_trip_id;") $oracle_cxn;  

keep if fishing_year>=2006 & fishing_year<=2008;
tempfile t1 reg_use2006;  
rename permit_debited permit_seller;
rename right_id right_id_seller;
rename permit_credited permit_buyer;
keep if allocation_use_type=="TRIP";
notes: This has ONLY TRIPS. This has no PTU in it.;
save `reg_use2006', replace;





/*DAS2.private_transaction_use
C. Trips that used leased DAS */
clear;

odbc load,  exec("select du.das_trip_id, du.allocation_use_type, du.au_date_time_debited, du.pt_permit_debited, du.permit_debited, du.quantity, du.category_name, du.plan, du.right_id, du.credit_type, 
du.fishing_year, activity_code, dt.permit_num, dt.sailing_port, dt.sailing_state, dt.trip_start, dt.trip_end, dt.landing_port, dt.landing_state, nvl(dt.external_charge,0) as external_charge
 from das2.private_transaction_use du, das2.das_trip dt
 where du.category_name='A' and du.plan='MUL'  and du.quantity<>0
 and du.das_trip_id=dt.das_trip_id;") $oracle_cxn;  

/*

*odbc load,  exec("select * from das2.private_transaction_use where plan='MUL' and category_name='A' and allocation_use_type='LEASE' ;") $oracle_cxn;  
rename permit_debited permit_seller;
rename right_id right_id_seller;
rename pt_permit_debited permit_buyer;
gen date_of_trade=dofc(au_date_time);
format date_of_trade %td;

/*fillin the right id of the buyer */
/* there's a couple of mis-matches.  some are date based*/
replace date_of_trade=mdy(11,5,2007) if permit_seller==230105 & permit_buyer==230222 & date_of_trade==mdy(11,6,2007) ;
merge m:1 date_of_trade permit_seller permit_buyer right_id_seller using `das2_leases';
replace right_id_buyer=760 if right_id_seller==711 & permit_seller==320311 & permit_buyer==330236 & fishing_year==2008;
drop if _merge==2;
keep quantity-date_of_trade right_id_buyer;
drop uc dc date_allocated;
gen date_charged=dofc(ptu_date);
format date_charged %td;

*/

keep if fishing_year>=2006 & fishing_year<=2008;
append using `reg_use2006';
rename permit_num permit;
rename quantity charge;
rename trip_start date_sail;
rename trip_end date_land;
drop plan category;
replace date_land=mdyhms(9,25,2008,15,21,0) if year(dofc(date_land))>=3008;

collapse (sum) charge external_charge, by (das_trip_id permit fishing_year activity_code sailing_port sailing_state date_sail date_land landing_port landing_state);
gen schema="DAS2";

save `das2_usage', replace;

append using `das1_usage';



gen marker=_n;
count;


joinby activity_code using $my_workdir/das2_activity_codes_augmented.dta, unmatched(master);

/* make sure I have all my observations */

gen flag=0;
replace flag=1 if date_sail>=start_date;
replace flag=0 if date_sail>end_date;
replace flag=1 if _merge==1;
bysort marker: egen tot=total(flag);
drop if tot==0 & permit==410163 & schema=="DAS" & _merge==3 & area_type=="differential";
replace flag=1 if tot==0;
keep if flag==1;
sort marker;
gen check=marker-marker[_n-1];
assert check==1 | (check==. & _n==1);
drop check marker flag tot;
drop _merge start_date end_date;
save `das2_usage', replace;









clear;
odbc load,  exec("select * from AMS.TRIP_AND_CHARGE where fmp='MULT' and DAS_TYPE='A-DAYS' and charge<>0 and fishing_year>=2009;") $oracle_cxn;  
drop running_clock observer rsa mult_override fmp trip_de-charge_uc trip_source fishing_area das_type tc_id charge_type; 
rename das_id ams_das_id;
rename trip_id ams_trip_id;
rename permit_nbr permit; 
destring, replace;
compress;
notes: charge is denominated in days. There is one entry that is negative. I don't think it's a data error. ;
 collapse (sum) charge, by(ams_das_id ams_trip_id permit date_s date_l fishing activity_code);
 gen schema="AMS";
 pause;
tempfile ams; 
save `ams', replace;
 



use $my_workdir/ams_activity_codes.dta, replace;
keep if fmp=="MULT";
keep activity_code activity_description charge_name das_category;
gen str4 schema="AMS";

tempfile ams_das2;
save `ams_das2';

use `ams', replace;
merge m:1 schema activity_code using `ams_das2', keep( 1 3);
assert _merge==3;
drop _merge;
save `ams', replace;
append using `das2_usage';


gen fishing_hours=hours(date_land-date_sail);

/* I'm making this way too hard.
1. For any DAS2 entries with activity code with Accounting=DURATION, set 
are_type 	start_date		end_date		time_factor
regular		01may2004 00:00:00	30apr2006		1
differential	01may2006 00:00:00	21nov2006 00:00:00	1.4
differential 	22nov2006 00:00:00				2

2. for any DAS2 entries with activity code with accounting=FIXED INTERVAL
	area=regular is always 3lower /15upper /15 fixed / and 1 to one
differential	MUL	A	3	11	15	1.4	01-MAY-04	21-NOV-06
differential	MUL	A	3	7.5	15	2	22-NOV-06	
*/


/* duration types */
gen charge_by_hand=fishing_hours;
/* Duration differential*/
/*DAS and DAS2 schema */
replace charge_by_hand=fishing_hours*1.4 if area_type=="differential" & accounting=="DURATION" & schema~="AMS";
replace charge_by_hand=fishing_hours*2 if area_type=="differential" & accounting=="DURATION" & date_sail>=clock("11/22/2006 00:00:00", "MDYhms")& schema~="AMS";






/* Fixed interval */
/* datesail is between May 1, 2006 and Nov 21, 2006  */
/* regular trips less than 3 hours */  
replace charge_by_hand=fishing_hours*1 if accounting=="FIXED INTERVAL" & area=="regular" & date_sail>=clock("05/01/2006 00:00:00", "MDYhms") & date_sail<clock("11/22/2006 00:00:00", "MDYhms") & fishing_hours<3 ;
/* differential trips less than 3 hours */  
replace charge_by_hand=fishing_hours*1.4 if accounting=="FIXED INTERVAL" & area=="differential"  & date_sail>=clock("05/01/2006 00:00:00", "MDYhms") & date_sail<clock("11/22/2006 00:00:00", "MDYhms") & fishing_hours<3 ;


/* regular trips more than 15 hours */  
replace charge_by_hand=fishing_hours*1 if accounting=="FIXED INTERVAL" & area=="regular" & date_sail>=clock("05/01/2006 00:00:00", "MDYhms") & date_sail<clock("11/22/2006 00:00:00", "MDYhms") & fishing_hours>=15 ;
/* differential trips more than 11 hours */  
replace charge_by_hand=fishing_hours*1.4 if accounting=="FIXED INTERVAL" & area=="differential"  & date_sail>=clock("05/01/2006 00:00:00", "MDYhms") & date_sail<clock("11/22/2006 00:00:00", "MDYhms") & fishing_hours>=11; 


/* regular trips between 3 and 15*/  
replace charge_by_hand=15 if accounting=="FIXED INTERVAL" & area=="regular" & date_sail>=clock("05/01/2006 00:00:00", "MDYhms") & date_sail<clock("11/22/2006 00:00:00", "MDYhms") & fishing_hours>=3 & fishing_hours<=15 ;
/* regular trips between 3 and 11*/  
replace charge_by_hand=15 if accounting=="FIXED INTERVAL" & area=="differential" & date_sail>=clock("05/01/2006 00:00:00", "MDYhms") & date_sail<clock("11/22/2006 00:00:00", "MDYhms") & fishing_hours>=3 & fishing_hours<=11 ;






/* Fixed interval */
/* datesail is after nov 22, 2006  and on/before April 30, 2009 */
/* regular trips less than 3 hours */  
replace charge_by_hand=fishing_hours*1 if accounting=="FIXED INTERVAL" & area=="regular" & date_sail>=clock("11/22/2006 00:00:00", "MDYhms") & fishing_hours<3 & schema=="DAS2";
/* differential trips less than 3 hours */  
replace charge_by_hand=fishing_hours*2 if accounting=="FIXED INTERVAL" & area=="differential"  & date_sail>=clock("11/22/2006 00:00:00", "MDYhms") & fishing_hours<3  & schema=="DAS2";


/* regular trips more than 15 hours */  
replace charge_by_hand=fishing_hours*1 if accounting=="FIXED INTERVAL" & area=="regular" & date_sail>=clock("11/22/2006 00:00:00", "MDYhms") & fishing_hours>=15 & schema=="DAS2";
/* differential trips more than 7.5hours */  
replace charge_by_hand=fishing_hours*2 if accounting=="FIXED INTERVAL" & area=="differential"  & date_sail>=clock("11/22/2006 00:00:00", "MDYhms") & fishing_hours>=7.5 & schema=="DAS2"; 



/* regular trips between 3 and 15*/  
replace charge_by_hand=15 if accounting=="FIXED INTERVAL" & area=="regular" & date_sail>=clock("11/22/2006 00:00:00", "MDYhms") & fishing_hours>=3 & fishing_hours<=15 & schema=="DAS2";
/* regular trips between 3 and 11*/  
replace charge_by_hand=15 if accounting=="FIXED INTERVAL" & area=="differential" & date_sail>=clock("11/22/2006 00:00:00", "MDYhms")  & fishing_hours>=3 & fishing_hours<=7.5 & schema=="DAS2";


/* now just need to deal with AMS FY2009 and beyond.
using charge_name DAY GILLNET,  GOMDA DAY GILLNET SNEDA, DAY GILLNET MULT
 */
 
 /* Duration differential*/
/*AMS schema */
replace charge_by_hand=fishing_hours*2 if inlist(charge_name,"GOMDA", "SNEDA", "HOOK - SNEDA") & schema=="AMS";

 /* Fixed interval */
/*AMS schema for Day Gillnet*/
replace charge_by_hand=15 if inlist(charge_name,"DAY GILLNET - MULT") & date_sail>=clock("05/01/2009 00:00:00", "MDYhms") & fishing_hours>=3 & fishing_hours<=15 & schema=="AMS";

/*AMS schema for Day Gillnet in a differential Area*/
replace charge_by_hand=15 if inlist(charge_name,"DAY GILLNET - GOMDA","DAY GILLNET - SNEDA") & date_sail>=clock("05/01/2009 00:00:00", "MDYhms") & fishing_hours>=3 & fishing_hours<=7.5 & schema=="AMS";
replace charge_by_hand=2*fishing_hours if inlist(charge_name,"DAY GILLNET - GOMDA","DAY GILLNET - SNEDA") & date_sail>=clock("05/01/2009 00:00:00", "MDYhms") &  fishing_hours>=7.5 & schema=="AMS";


replace charge_by_hand=charge_by_hand/24;
replace fishing_hours=fishing_hours/24;
rename fishing_hours fishing_days;

/*  in 2010 and after, DAS get charged in 24 hour increments for common pool vessels. Not exactly sure what is done for sector vessels, but whever*/
replace charge_by_hand=ceil(charge_by_hand) if date_sail>=clock("05/01/2010 00:00:00", "MDYhms") & schema=="AMS"; 

order fishing_days charge_by_hand external_charge, after(charge);
compress;
rename das_trip_id das2_trip_id;
order das_transaction das2_trip_id ams_trip_id ams_das_id;
drop plan category_name;


gen day_gillnet=0 ;
replace day_gillnet=1 if strmatch(charge_name,"DAY GILLNET*")  & schema ~= "DAS";
replace day_gillnet=1 if strmatch(fishing_gear,"Day Gillnet*");
replace activity_desc=lower(activity_desc);
replace day_gillnet=1 if strmatch(activity_description,"*day gillnet*");
replace day_gillnet=1 if gillnet_vessel=="Y" & day_trip=="D" & schema=="DAS";

gen gillnet=0;
replace gillnet=1 if strmatch(activity_description,"*gillnet*") & schema~="DAS";
replace gillnet=1 if gillnet_vessel=="Y" & schema=="DAS";
replace gillnet=1 if strmatch(charge_name,"*GILLNET*") & schema~="DAS";
replace gillnet=1 if inlist(fishing_gear,"Day Gillnet", "Gillnet", "Trip Gillnet", "gillnet")  & schema~="DAS";
replace gillnet=0 if strmatch(activity_description,"*non gillnet*") & schema~="DAS";
replace gillnet=0 if inlist(fishing_gear,"Hook Gear", "Non-Gillnet", "non-gillnet") & schema~="DAS";
replace gillnet=1 if strmatch(activity_code,"MNK-GIL") & schema~="DAS";

save $my_workdir/das_usage_$today_date_string.dta, replace;










/*


notes: the schema column lets me know where the data came from.  Not all fields were collected in all three schema. ;
notes: Permit, charge, fishing_year, date_sail, date_land, fishing_year and activity_code are in all;
notes: sailing port, state, landings port state, gillnet, and observer were only databased in AMS.;
notes: DAS2 contains right_ids, but DAS and AMS do not.; 
compress;

replace date_land=mdyhms(9,25,2008,15,21,0) if year(dofc(date_land))>=3008;










use $my_workdir/das2_activity_codes.dta, replace;
gen str4 schema="DAS2";
append using `ams_das2';
compress;
save `ams_das2', replace;

use $my_workdir/das_usage_$today_date_string.dta, clear;
merge m:1 activity_code schema using `ams_das2', keep(1 3);
/* _merge==1 is DAS schema. 
_merge==2 is codes without trips. Like B days.
_merge==3 is what we want

*/

pause;

gen manual_charge_hours=hours(date_land-date_sail);
gen multipier=1;
replace multipier=1.4 if date_sail>=cofd(date("05/01/2006", "MDY") & date_sail<=cofd(date("11/21/2006", "MDY") & inlist(charge_name,"DAY GILLNET - GOMDA", "DAY GILLNET - SNEDA","GOMDA", "SNEDA", "HOOK - SNEDA");


drop activity_description fishing_gear charge_name das_category _merge;
saveold $my_workdir/das_usage_$today_date_string.dta, replace version(12);

*/





/*



clear;


















/* DAS schema data*/

/* leases */
odbc load, exec("select * from das.das_transfer_lease where fishery='MUL' and das_category='A' and TRANSACTION_TYPE='L' order by nmfs_approval_date desc;") $oracle_cxn;
keep if inlist(fishing_year, 2004, 2005);
rename grantor_right_to_days_id right_id_seller;
rename grantee_right_to_days_id right_id_buyer;

rename grantor_permit_number permit_seller;
rename grantee_permit_number permit_buyer;
drop user_changed date_changed user_entered transaction_type; 
replace nmfs_approval_date=dofc(nmfs_approval_date);
replace date_entered=dofc(date_entered);

format nmfs_approval_date date_entered %td;
rename das_leased quantity;
rename das_price dollar_value;
rename nmfs_approval date_of_trade;
drop fishery date_entered das_category;
compress;
tempfile dl1;
gen schema="DAS";


save `dl1';

/*DAS2.allocation_use
 B. LEASES*/
clear;
odbc load,  exec("select * from das2.allocation_use where category_name='A' and plan='MUL' and allocation_use_type='LEASE' and approval_status='APPROVED' ;") $oracle_cxn;  
keep if fishing_year>=2006 & fishing_year<=2008;

gen date_of_trade=dofc(au_date_time);
format date_of_trade %td;
collapse (sum) quantity (first) dollar_value, by(permit_d permit_c right_id right_id_c date_of_trade fishing_year);
rename permit_d permit_seller;
rename permit_c permit_buyer;
rename right_id_c right_id_buyer;
rename right_id right_id_seller;
order date fishing_year;
sort fishing_year date permit_s;

/* This will lookup the right_id from MQRS. It's just a few corrections, so it's easier to do by hand.
select a.*, b.right_id as mqrs_right_id from
    (select au_date_time_debited, permit_debited, permit_credited, quantity, right_id, right_id_credited from das2.allocation_use
    where category_name='A' and plan='MUL' and allocation_use_type='LEASE' and approval_status='APPROVED' and right_id_credited is NULL
    ) a, 
   (select per_num, date_eligible, date_cancelled, right_id from mort_elig_criteria where fishery='MULTISPECIES' and per_num in
    (select distinct permit_credited from das2.allocation_use
    where category_name='A' and plan='MUL' and allocation_use_type='LEASE' and approval_status='APPROVED' and right_id_credited is NULL
    )) b 
      where a.permit_credited=b.per_num and 
      a.au_date_time_debited between b.date_eligible and b.date_cancelled; 
*/



replace right_id_buyer=1807 if right_id_buyer==. & permit_buyer==121546;
replace right_id_buyer=559 if right_id_buyer==. & permit_buyer==310912;
replace right_id_buyer=455 if right_id_buyer==. & permit_buyer==251364;
replace right_id_buyer=2055 if right_id_buyer==. & permit_buyer==149334;
compress;

tempfile das2_leases;
gen schema="DAS2";

save `das2_leases', replace;


/*AMS lease data */

odbc load,  exec("select lease_exch_id,from_permit, to_permit, from_right, to_right, fishing_year, quantity, price, approval_date from ams.lease_exch_applic@das08_11g.nero.gl.nmfs.gov 
	where FMP='MULT' and from_das_type='A-DAYS' and approval_status='APPROVED' and fishing_year>=2009;") dsn("cuda") user(mlee) password($mynero_pwd) lower clear;
destring, replace;
rename to_permit permit_buyer;
rename from_permit permit_seller;
rename to_right right_id_buyer;
rename from_right right_id_seller;
compress;
rename approval_date date_of_trade;
rename price dollar_value;
replace date_of_trade=dofc(date_of_trade);
format date_of_trade %td;


gen schema="AMS" ;



/*stack on DAS2 and DAS leases */
append using `das2_leases';
append using `dl1';
/* fix another broken entry */

replace date=mdy(month(date),day(date),2004)    if transfer_id==815
saveold $my_workdir/leases_$today_date_string.dta, replace version(12);



*/


