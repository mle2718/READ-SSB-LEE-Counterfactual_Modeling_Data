#delimit;


	clear;
	odbc load, exec("select * from MQRS.SECTOR_PARTICIPANTS_CPH;")  $oracle_cxn;  
	gen dbyear=2010;                  
tempfile mm;
save `mm';

forvalues yr=$secondyr/$lastyr{ ;
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




forvalues yr=$firstyr/$lastyr{ ;
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
