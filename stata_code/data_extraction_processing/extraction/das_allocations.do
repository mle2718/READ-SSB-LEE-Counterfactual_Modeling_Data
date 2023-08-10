#delimit;
clear;
odbc load,  exec("select * from das.das_allocation where fishery='MUL' and das_category='A' and fishing_year between 2004 and 2005;") $oracle_cxn;  
rename fishery fmp;
rename das_category das_type;
rename right_to_days_id mri;
keep if transfer_id==.;
keep mri das_net_allocation fishing_year;

rename das_net categoryA_DAS;
tempfile das;
*saveold $my_workdir/das_allocation_$today_date_string.dta, replace version(12);
save `das';


clear;
odbc load,  exec("select * from das2.allocation where plan='MUL' and category_name='A';") $oracle_cxn;  
rename plan fmp;
rename category_name das_type;

destring, replace;
keep if fishing_year>=2006 & fishing_year<=2008;

tempfile das2;
collapse (sum) quantity, by(right_id credit_type fishing_year);
compress;
save `das2', replace;

/*AMS -- 2007 to Present */
clear;

odbc load,  exec("select *  from ams.allocation_tx where FMP='MULT' and das_type='A-DAYS';") $oracle_cxn;  
destring, replace;
keep if fishing_year>=2009;

collapse (sum) quantity, by(fishing_year allocation_type root_mri);
rename allocation_type credit_type;
rename root_mri right_id;
compress;
append using `das2';
rename right_id mri;
 drop if inlist(credit_type,"LEASE IN", "LEASE OUT");
 collapse (sum) quantity, by(mri fishing_year);
 rename quantity categoryA_DAS;
save `das2', replace;



append using `das';
sort mri fishing_year;
save $my_workdir/das_allocations_$today_date_string.dta, replace ;
