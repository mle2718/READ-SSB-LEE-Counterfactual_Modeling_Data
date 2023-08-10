

/* don't bother with this */
/* don't bother with this */
/* don't bother with this */
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


quietly do "/home/mlee/Documents/Workspace/technical folder/do file scraps/odbc_connection_macros.do";
global oracle_cxn "conn("$mysole_conn") lower";
local date: display %td_CCYY_NN_DD date(c(current_date), "DMY");
global today_date_string = subinstr(trim("`date'"), " " , "_", .);
clear;

tempfile das1_transfer das2_transfer ams_transfer;





clear;
/* DAS schema data*/
/* usage */

odbc load,  exec("select * from das_transfer_no_lease where fishery='MUL' and fishing_year between 2004 and  2005) $oracle_cxn;  
	keep if inlist(fishing_year, 2004, 2005);
gen schema="DAS";

save `das1_transfer',replace;
clear;


