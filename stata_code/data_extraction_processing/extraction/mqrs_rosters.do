/* don't do this anymore */

#delimit;

*log using "AB_extraction.smcl", replace;


clear;
odbc load, exec("select * from mqrs.mort_elig_criteria where fishery='MULTISPECIES' and (date_eligible<>date_cancelled or date_cancelled is null)
	and (date_cancelled>=to_date('01/01/2004','MM/DD/YYYY') or date_cancelled is null);") $oracle_cxn;
/*there are two lines that are causing trouble in MQRS */
drop if inlist(app_num,1227709,1220763); 

keep per_num vessel hull_id right_id auth_id date_eligible date_cancelled cancel_reason_code elig_status grp_num auth_type right_id auth_id;
rename per_num vp_num;

keep vp_num hull_id date_eligible date_cancelled auth_type auth_id right_id;
tempfile mqrs_test;
save `mqrs_test', replace;
clear;

/*This is a bit of a hack---there are some entries in MQRS that had messed up YEARS. these were fixed.but still, this code cannot hurt. */


odbc load, exec("select per_num, app_num, vessel, hull_id, date_eligible, cancel_reason_code, elig_status, grp_num, auth_type, right_id, auth_id ,to_char(date_cancelled) as date_cancelled 
	from mqrs.mort_elig_criteria where fishery='MULTISPECIES' and right_id in ('1683', '47836', '47960', '48000') and extract(year from date_cancelled)< 20;") $oracle_cxn;

qui forvalues y=1/9{;
	replace date_cancelled=subinstr(date_cancelled,"000`y'","200`y'",1);
};

qui forvalues y=10/99{;
	replace date_cancelled=subinstr(date_cancelled,"00`y'","20`y'",1);
};


gen double mydc=cofd(date(date_cancelled,"DMY"));
format mydc %tc;
drop date_cancelled;
drop if inlist(app_num,1227709,1220763); 

rename mydc date_cancelled;
rename per_num vp_num;
append using `mqrs_test';
keep vp_num hull_id date_eligible date_cancelled auth_type auth_id right_id;
dups, drop terse;
save `mqrs_test',replace;

/* odbc load, exec("select per_num, vessel, right_id,to_string(date_cancelled) as date_cancelled from mqrs.mort_elig_criteria where fishery='MULTISPECIES' and right_id in ('1683', '47836', '47960', '48000') and extract(year from date_cancelled)< 20;") $oracle_cxn;
*/
clear;


odbc load, exec("select * from mqrs.sector_year_roster ") $oracle_cxn;
drop de uc dc;
rename mri right_id;
rename year fishing_year;
keep sector_id fishing_year right_id start_date end_date;
qui count;
scalar syr=r(N);
tempfile syr;
save `syr';

joinby right_id using `mqrs_test';




foreach var of varlist date_eligible date_cancelled start_date end_date{;
	replace `var'=dofc(`var');
	format `var' %td;
};


keep if  date_eligible<=mdy(05,01,fishing_year) & (date_cancelled>=mdy(05,01,fishing_year) | date_cancelled==.);

notes sector_id: This comes from MQRS.SECTOR_YEAR_ROSTER;
notes fishing_year:This comes from MQRS.SECTOR_YEAR_ROSTER;
notes right_id : This comes from MQRS.SECTOR_YEAR_ROSTER;
notes start_date: This comes from MQRS.SECTOR_YEAR_ROSTER;
notes  end_date:This comes from MQRS.SECTOR_YEAR_ROSTER;

notes vp_num : This comes from mort_elig_criteria;
notes hull_id : This comes from mort_elig_criteria;

notes date_eligible : This comes from mort_elig_criteria;
notes date_cancelled: This comes from mort_elig_criteria;
notes auth_type: This comes from mort_elig_criteria;
notes auth_id: This comes from mort_elig_criteria;
notes: This was constructed from MQRS.SECTOR_YEAR_ROSTER and MQRS.MORT_ELIG_CRITERIA.  ;


qui count;
scalar m=r(N);

keep sector_id fishing_year right_id vp_num hull_id auth_type auth_id ;
save $my_workdir/sector_year_roster_$today_date_string.dta, replace;


/*These is a data check.  
1. Do I have the right number of observations?
2. Do I have the right observations? */


/* 1  */

*assert m==syr;


/*2. Do I have the right observations?
keep sector fishing_year right_id;
gen source=1;
tempfile stack1 ;
save `stack1';


use `mqrs_test', clear;
save $my_workdir/mqrs_raw_$today_date_string.dta, replace;

use `syr', clear;
save $my_workdir/syr_$today_date_string.dta, replace;

gen source=0;
append using `stack1';
bysort sector right fishing_year: gen count=_N;
count if count==1;
assert r(N)==0;
 */

/* These 4 MRI's got kicked out of their sector.  This means they could not fish for groundfish after the end_date for the rest of that fishing year. 
sector_id	year	mri	start_date	ue		end_date	end_reason
19		2012	3327	01may2012	sector	26mar2013 10:51:00	expulsion
19		2012	2692	01may2012	sector	26mar2013 10:51:00	expulsion
13		2012	1434	01may2012	sector	13feb2013 00:00:00	expulsion
19		2012	3015	01may2012	sector	26mar2013 10:51:00	expulsion
*/



/* There are 4 MRIs that are in sector_year_roster but NOT in MQRS
This is because their years were entered as 17 or 16 instead of 2016 or 2017

*/


