#delimit;
/*This is code to extract key-files or support tables. 
Part A. PLAN and Category - DOne
Part B. Species Codes -DONE
Part C. Port codes

*/


/* Part A. PLAN and Category */
clear;
odbc load, exec("select fishery_id, plan, cat, permit_year, descr, moratorium_fishery, mandatory_reporting, per_yr_start_date, per_yr_end_date from valid_fishery;")  $oracle_cxn; 
renvars, lower;
notes: made by "fishery_key_file.do";
notes: moratorium_fishery=T means limited access. You can also verify this from the "descr" field. ;
notes: While SF and OQ (Surfclam and Ocean Quahog) are open acces, they are also IFQ fisheries. Anyone can get an open access permit, but an operator would need to hold IFQ to fish for surfclam or ocean quahog;
saveold $my_workdir/fishery_keyfile_$today_date_string.dta, replace version(12);


/* Part B. Species Codes*/
clear;
odbc load, exec("select nespp4, species_itis, common_name, scientific_name, unit_of_measure, grade_code, grade_desc, market_code, market_desc, cf_lndlb_livlb, cf_rptqty_lndlb from species_itis_ne;")  $oracle_cxn; 
destring, replace;
compress;
renvars, lower;
notes: made by "fishery_key_file.do";

saveold  $my_workdir/species_keyfile_$today_date_string.dta, replace version(12);


clear;
odbc load, exec("select sppnm, mktnm, nespp3, sppnm3, nespp4, necnv as cf_lndlb_livlb from cfspp;")  $oracle_cxn; 
destring, replace;
compress;
renvars, lower;
dups, drop terse;
drop if nespp3<=0;
replace sppnm="SKATES" if nespp3==365;
replace sppnm="AMBERJACK" if nespp3==181;
drop _expand;
notes: made by "fishery_key_file.do";

saveold  $my_workdir/nespp4_keyfile_$today_date_string.dta, replace version(12);





clear;
odbc load, exec("select sppcode, nespp3, nespp4, sppname, sppconv, comments from vlsppsyn;")  $oracle_cxn; 
destring, replace;
compress;
renvars, lower;
dups, drop terse;
drop if nespp4<=-1;
drop _expand;
sort nespp3 nespp4;
notes: made by "fishery_key_file.do";

saveold $my_workdir/vlspp_keyfile_$today_date_string.dta, replace version(12);
