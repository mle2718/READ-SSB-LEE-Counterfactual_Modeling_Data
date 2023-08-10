

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

tempfile das2_usage das1_usage reg_use2006 das2a initial;





clear;
/* DAS schema data*/
/* usage */

odbc load,  exec("select du.fishing_year, du.das_transaction_id, du.permit_number, du.das_charged_days, tr.sailing_port, tr.sailing_state, tr.sail_date_time as date_sail, tr.landing_port, tr.landing_state, tr.landing_date_time as date_land,
		tr.gillnet_vessel, tr.day_trip, tr.observer_onboard, tr.das_charged_fixed, tr.fishery_code, tr.vessel_name 		
	from das.das_used du, das.trips tr where du.das_transaction_id=tr.das_transaction_id and du.permit_number=tr.permit_number and du.das_category='A' and du.fishery='MUL';") $oracle_cxn;  
	keep if inlist(fishing_year, 2004, 2005);
rename permit permit;
rename das_charged_days charge;
rename fishery_code activity_code;

collapse (sum) charge (first) gillnet_vessel day_trip observer_onboard das_charged_fixed vessel_name , by(fishing_year permit sailing_port sailing_state date_sail landing_port landing_state date_land activity_code );
gen schema="DAS";

save `das1_usage',replace;




clear;









/*DAS2 schema DAS2.allocation_use
 A. Trips */
odbc load,  exec(" select du.das_trip_id, du.allocation_use_type, du.au_date_time_debited, du.permit_debited, du.permit_credited, du.quantity, du.category_name, du.plan, du.right_id, du.credit_type, 
du.fishing_year, du.dollar_value, activity_code, dt.permit_num, dt.sailing_port, dt.sailing_state, dt.trip_start, dt.trip_end, dt.landing_port, dt.landing_state
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
du.fishing_year, activity_code, dt.permit_num, dt.sailing_port, dt.sailing_state, dt.trip_start, dt.trip_end, dt.landing_port, dt.landing_state
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

collapse (sum) charge, by (au_date_time_debited permit_debited permit right_id fishing_year activity_code sailing_port sailing_state date_sail date_land landing_port landing_state);
gen schema="DAS2";

save `das2_usage', replace;

clear;
odbc load,  exec("select * from AMS.TRIP_AND_CHARGE where fmp='MULT' and DAS_TYPE='A-DAYS' and charge<>0 and fishing_year>=2009;") $oracle_cxn;  
drop running_clock observer rsa mult_override fmp trip_de-charge_uc trip_source fishing_area das_type das_id trip_id tc_id charge_type; 

rename permit_nbr permit;
destring, replace;
compress;
notes: charge is denominated in days. There is one entry that is negative. I don't think it's a data error. ;
 collapse (sum) charge, by(permit date_s date_l fishing activity_code);
 gen schema="AMS";

append using `das2_usage';
append using `das1_usage';


notes: the schema column lets me know where the data came from.  Not all fields were collected in all three schema. ;
notes: Permit, charge, fishing_year, date_sail, date_land, fishing_year and activity_code are in all;
notes: sailing port, state, landings port state, gillnet, and observer were only databased in AMS.;
notes: DAS2 contains right_ids, but DAS and AMS do not.; 
compress;

replace date_land=mdyhms(9,25,2008,15,21,0) if year(dofc(date_land))>=3008;
save $my_workdir/das_usage_$today_date_string.dta, replace ;


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
clear;
odbc load,  exec("select lease_exch_id,from_permit, to_permit, from_right, to_right, fishing_year, quantity, price, approval_date from ams.lease_exch_applic@sole 
	where FMP='MULT' and from_das_type='A-DAYS' and approval_status='APPROVED' and fishing_year>=2009;") $oracle_cxn;  
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

replace date=mdy(month(date),day(date),2004)    if transfer_id==815;
save $my_workdir/leases_$today_date_string.dta, replace ;






