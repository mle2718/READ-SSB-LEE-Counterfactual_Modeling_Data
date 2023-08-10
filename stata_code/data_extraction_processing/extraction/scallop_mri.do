/* THIS BIT OF CODE IS USED TO EXTRACT VESSEL PERMITS*/
#delimit;

clear;
	odbc load,  exec("select * from mlee.IFQ_MRI;") $oracle_cxn;  
gen das_ifq=1;
tempfile dual;
rename mri right_id;
save `dual', replace;



 clear;
odbc load,  exec("select right_id, hull_id, vessel, date_eligible, date_cancelled, auth_type, fishery from mqrs.mort_elig_criteria mq  
where fishery in ('SCALLOP','GENERAL CATEGORY SCALLOP') AND date_eligible is not null 
AND (date_cancelled>=to_date('05/01/2000','MM/DD/YYYY') or date_cancelled is null) 
AND ((date_cancelled > date_eligible) or date_cancelled is null);") $oracle_cxn;  


    destring, replace;
    renvars, lower;
    

	merge m:1 right_id using `dual';
	keep if _merge==3;
	drop _merge;
	sort right_id date_eligible date_cancelled;
	
	
	saveold $my_workdir/dual_scallops_$today_date_string, version(12) replace;

/*I want to make a dataset that contains the VP_NUM, FY, PLANS_CATS that were active in a FY */

/* I'll make a set of dummies that is 
1 if the permit start_date was before the end of the fishing year AND the permit end_date was after the beginning of the fishing year
*/
















/*

forvalues j=1996(1)2016{;
	gen a`j'=0;
	local k=`j'+1;
	replace a`j'=1 if start_date<mdy(5,1,`k') & myde>=mdy(5,1,`j');
};

collapse (sum) a1996-a2016, by(vp_num plan cat);
foreach var of varlist a*{;
	replace `var'=1 if `var'>=1;
};
reshape long a, i(vp_num plan cat) j(fishing_year);

replace a=1 if a>=1;
drop if a==0;

/*save permits, replace; */
gen plancat=plan + "_" + cat;
drop plan cat;
reshape wide a, i(vp_num fishing_year) j(plancat) string;

foreach var of varlist a*{;
	replace `var'=0 if `var'==.;
	};
renvars a*, predrop(1);

rename vp_num permit;
qui compress;
notes: made by "permit_characteristics_extractions.do";
saveold $my_workdir/permit_working_$today_date_string.dta, replace version(12);




/* THIS BIT OF CODE IS USED TO EXTRACT VESSEL Characteristics*/


clear;
odbc load, exec("select ap_year, ap_num, vp_num, hull_id, ves_name, strt1, strt2, city, st, zip1, zip2, tel, hport, hpst, pport, ppst, len, crew, gtons, 
ntons, vhp, blt, hold, toc,top, date_issued, date_canceled, max_trap_limit
from vps_vessel where ap_year>=1996;") $oracle_cxn;
    destring, replace;
    renvars, lower;
    gen mys=dofc(date_issued);
    gen myend=dofc(date_canceled);
drop date_issued date_canceled;

rename mys date_issued;
rename myend date_canceled;
format date* %td;

tempfile permit_working;
save `permit_working', replace;

forvalues j=1996(1)2016{;
	tempfile new;
	local NEWfiles `"`NEWfiles'"`new'" "'  ;

	use `permit_working', clear;
	gen a`j'=0;
	local k=`j'+1;
	replace a`j'=1 if date_issued<mdy(5,1,`k') & date_canceled>=mdy(5,1,`j');
	bysort vp_num a`j' (ap_num): replace a`j'=0 if _n<_N;
	keep if a`j'==1;
	keep vp_num-top max_trap_limit;
	gen fishing_year=`j';
	save `new';
	clear;
};

append using `NEWfiles';



rename vp_num permit;



/* This bit joins them */

merge 1:1 permit fishing_year using $my_workdir/permit_working_$today_date_string;
saveold $my_workdir/permit_portfolio_$today_date_string, version(12) replace;


keep hull_id fishing_year max_trap_limit;

saveold $my_workdir/lobster_traps_$today_date_string, version(12) replace;



*/



