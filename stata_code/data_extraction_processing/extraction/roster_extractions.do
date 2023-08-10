#delimit;
clear;
odbc load, exec("select * from sector.mv_mri_permit_vessel_history where latest_entry_by_year='Y';") $oracle_cxn;
rename fy_year fishingyear;
rename sector_id sims_sector_id;
rename vessel_name ves_name;
rename vessel_permit_num permit;
rename vessel_hull_id hullid;
order fishingyear mri sims_sector_id sector_name ves_name permit auth_type hullid; 

/* a few duplicates */
bysort fishingyear mri (start_date): gen obsno=_n;
count if obsno>=2;
assert r(N)==5;
drop if obsno==2;
drop obsno;
saveold $my_workdir/sector_participants_$today_date_string.dta, replace version(12);


/* old code

	clear;
	odbc load, exec("select * from MQRS.SECTOR_PARTICIPANTS_CPH;")  $oracle_cxn;  
	gen dbyear=2010;                  
tempfile mm;
save `mm';

forvalues yr=$secondyr/2019{ ;
	tempfile posfix;
	local NEWpos`"`NEWpos'"`posfix'" "'  ;
	clear;
	odbc load, exec("select * from MQRS.SECTOR_PARTICIPANTS_CPH`yr';")  $oracle_cxn;                    
	gen fishingyear= `yr';
	quietly save `posfix';
};
dsconcat `NEWpos' `mm';
destring, replace;
compress;
notes: made by "roster_extractions_do" ;
saveold $my_workdir/sector_participants_$today_date_string.dta, replace version(12);




forvalues yr=$firstyr/2019{ ;
	tempfile roster;
	local NEWroster`"`NEWroster'"`roster'" "'  ;
	clear;
	odbc load, exec("select * from MQRS.SECTOR_YEAR_ROSTER`yr';")  $oracle_cxn;                    
	gen fishingyear= `yr';
	quietly save `roster';
};
dsconcat `NEWroster';
destring, replace;
compress;
notes: made by "roster_extractions_do" ;
saveold $my_workdir/sector_year_roster$today_date_string.dta, replace version(12);
 */