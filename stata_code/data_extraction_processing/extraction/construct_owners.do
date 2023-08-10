
/* Min-Yang.Lee@noaa.gov */

/* Objective: This code is used to extract hull_id, business, id, and person_ids
*/



/* PRELIMINARIES */
/* Set up folders and oracle connection*/

version 15.1
pause off
#delimit ;





/***************************************************
1.  Construct Affiliates
***************************************************/
/* Port of chad's sas code to get ownership data.*/
/* Objective: Construct a key-file which contains VP_NUM, Affiliate_id, and ap_year.
The Affiliation variable is "constant" for all VP_NUMS which have the exact same person_id's associated with it.
Note: The affiliate_id number that is associated with an entity may change when this code is re-run and data are extracted again.  Caveat emptor.
Note2: There are some VP_NUM's that have revenue but no ownership information. These VP_NUMS are assigned an affiliate_id number in step 3.
*/

/* Min-Yang's comment: This code is slightly modified from Chad's SQL code.  It joins data from three tables (vps_owner, vps_fisher_ner, and business_owner)*/

clear;
odbc load,  exec("select distinct(b.person_id), b.business_name, c.business_id, a.vp_num, a.ap_year, ves.hull_id
	from permit.vps_owner c, permit.bus_own b, permit.vps_fishery_ner a, permit.vps_vessel ves
		where c.ap_num in (select max(ap_num) as ap_num from permit.vps_fishery_ner where ap_year between 2010 and 2015 group by vp_num, ap_year)
	 and c.business_id=b.business_id and a.ap_num=c.ap_num and a.ap_num=ves.ap_num;") $mysole_conn;


display "check1";
/* important to use bysort vp_num ap_year (person_id) to consistently order the person-id's within the groups defined by vp_num and ap_year*/
/* this just generates a numeric 'suffix' for the person_id variable.  For a given VP_NUM and YEAR, the lowest person_id has the lowest jid.
This is not important for now, but will be used in the next step when arraying person ids.*/
bysort vp_num ap_year (person_id ): gen jid=_n;

/* reshape the data to wide --- array out the person ids.  Sort the data by person_id1.  For entries with the same person_id1, sort by person_id2. Etc.  */

reshape wide person_id , i(vp_num ap_year) j(jid);
sort person_id*;

/* Generate affiliate_id variable: Observations which have the same value for affiliate_id have the same distinch pattern of person_ids.
egen group() constructs a new variable taking on values 1,2,3,...., for each distinct combination of the person_id variables. The missing option allows for a missing value to be matched.  */

assert person_id1<.;
egen affiliate_id=group(person_id*), missing;
order affiliate_id ap_year vp_num;
sort affiliate_id ap_year vp_num;

sort affiliate vp_num ap_year;

/* it should be impossible for a vp_num to have 2 affiliate_id's in a year.  Check this and break the program if there are vp_nums with 2 affiliated_ids. */
duplicates tag vp_num affiliate_id ap_year, gen(mytt9);
assert mytt9==0;
drop mytt9;
/* rename ap_year as year and vp_num as permit to facilitate joining to dealer data*/
rename vp_num permit;
order hull_id business_id business_name;

save "${my_workdir}/ownership2_${today_date_string}.dta", replace;

#delimit; 
clear;
odbc load,  exec("select pids.*, ves.hull_id from permit.owner_history_pids pids, permit.vps_vessel ves
     where  pids.ap_num in (select max(ap_num) as ap_num from permit.owner_history_pids group by permit, ap_year) 
	 and pids.ap_num=ves.ap_num;") $mysole_conn;
destring , replace;
drop if ap_year==.;
drop if ap_year<=2003;
bysort permit ap_year (person_id ): gen jid=_n;
drop ap_num;
reshape wide person_id name_last name_middle name_first name_suffix owner, i(permit ap_year hull_id) j(jid);
rename owner1 owner;
drop owner?;
drop owner??;

save "${my_workdir}/ownership1_${today_date_string}.dta", replace;



clear;

odbc load,  exec("select business_id, business_name, person_id, name_first, name_middle, name_last from permit.bus_own;") $mysole_conn;
destring, replace;
save "${my_workdir}/names_${today_date_string}.dta", replace;
