#delimit;
clear;
odbc load,  exec("select * from das.das_allocation where fishery='MUL' and das_category='A' and fishing_year between 2004 and 2005;") $oracle_cxn;  
rename fishery fmp;
rename das_category das_type;
rename right_to_days_id mri;
keep if transfer_id==.;
gen categoryA_DAS=das_base_allocation+das_carry_over;
keep mri categoryA_DAS  fishing_year;
collapse (sum) categoryA_DAS, by(mri fishing_year);
tempfile das;
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
append using `das';
save `das2', replace;
clear;












odbc load,  exec("select per_num, right_id, hull_id, vessel, date_eligible, date_cancelled, auth_type, remark, len, hp  from mqrs.mort_elig_criteria mq  
where fishery='MULTISPECIES' 
AND date_eligible is not null 
AND (date_cancelled>=to_date('05/01/2000','MM/DD/YYYY') or date_cancelled is null) 
AND ((date_cancelled > date_eligible) or date_cancelled is null);") $oracle_cxn;  
preserve;
/* hack the missing MQRS data
From Ted Hawes:
Hi Min-Yang,

It looks like these were corrected in 2009 as part of a clean-up project.  You can see the updated MRI for these MRIs using this script...I just changed right_id to auth_id. 

 I'm not familiar with the das2.allocation table.  If you want to discuss more, let me know.  

select * from mort_elig_criteria where auth_id in(1179,1183,1187,1196,1219,1255,1261,1293,1296,1362,1374,2423);  

 */
 clear;
odbc load,  exec("select per_num, right_id, auth_id, hull_id, vessel, date_eligible, date_cancelled, auth_type, remark, len, hp  from mqrs.mort_elig_criteria mq  
where fishery='MULTISPECIES' AND
auth_id in(1179,1183,1187,1196,1219,1255,1261,1293,1296,1362,1374,2423) 
AND date_eligible is not null 
AND (date_cancelled>=to_date('05/01/2000','MM/DD/YYYY') or date_cancelled is null) 
AND ((date_cancelled > date_eligible) or date_cancelled is null);") $oracle_cxn;  

replace right_id=auth_id;
tempfile e1;
save `e1', replace;
clear;

odbc load,  exec("select per_num, right_id, auth_id, hull_id, vessel, date_eligible, date_cancelled, auth_type, remark, len, hp  from mqrs.mort_elig_criteria mq  
where fishery='MULTISPECIES' AND
auth_id in(1174,1176,1179,1183,1184,1187,1196,1209,1298,1358,1372) 
AND date_eligible is not null 
AND (date_cancelled>=to_date('05/01/2000','MM/DD/YYYY') or date_cancelled is null) 
AND ((date_cancelled > date_eligible) or date_cancelled is null);") $oracle_cxn;  

replace right_id=auth_id;
append using `e1';
save `e1', replace;

restore;
append using `e1';



destring, replace;
renvars, lower;
compress;
sort right_id date_e date_c;
gen remark1=substr(remark,1,200);
gen remark2=substr(remark,201,.);
replace auth_type="CPH" if strmatch(auth_type,"HISTORY RETENTION");
drop remark;
replace auth_type="CPH" if auth_type=="HISTORY RETENTION" ;
dups, drop terse;
drop if right_id==1901 & dofc(date_eligible)==mdy(5,4,1994);
drop _expand;
saveold $my_workdir/mqrs_old_$today_date_string.dta, replace version(12);

#delimit;
drop remark* vessel hull;
dups, drop terse;
drop _expand;
forvalues j=2000(1)$lastyr{;
	gen a`j'=0;
	local k=`j'+1;
	replace a`j'=1 if dofc(date_eligible)<mdy(5,1,`k') & dofc(date_cancelled)>=mdy(5,1,`j');
	bysort right_id (date_eligible) : gen t`j'=sum(a`j');
	replace a`j'=0 if t`j'>1;
	drop t`j';
};

rename auth_type type_auth;
collapse (sum) a*, by(per right type_auth);


compress;
reshape long a, i(per_num right_id type_auth) j(fishing_year);
drop auth_id;
replace a=1 if a>=1;
drop if a==0;


drop a;
rename per_num permit;
qui compress;
notes: made by "mort_elig_criteria-extractions.do";
rename right_id mri;
merge 1:1 mri fishing_year using `das2';
keep if fishing_year>=2004;
replace categoryA_DAS=0 if categoryA_DAS==.;
drop if categoryA_DAS==0;
drop if type_auth=="";
notes: A permit number doesn't get detached from a MRI in CPH until that MRI comes out of CPH. An owner can sells a permit separate from history, putting the history into CPH.  If this happens there may be 2 links between a right_id and a permit. The actual one is the one NOT IN CPH.  ;

notes: CategoryA DAS are the "BASE"+"CARRYOVER" + "SANCTION" Days-at-Sea.  2004-2005 are from the DAS 2006-2008 are from DAS2 schema, 2009-present are from AMS.;
drop _merge;
saveold $my_workdir/mqrs_annual_$today_date_string.dta, replace version(12);
