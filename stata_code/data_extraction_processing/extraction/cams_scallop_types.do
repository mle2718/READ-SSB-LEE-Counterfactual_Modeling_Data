#delimit;

clear;
odbc load,  exec("select year, permit, activity_code_1, docid as tripid, vtr_imgid from cams_GARFO.CAMS_VTR_ORPHANS_SUBTRIP       
    where activity_code_1 like 'SES%';") $myNEFSC_USERS_conn;
renvars, lower;
tempfile orphans;
save `orphans';

clear;
odbc load,  exec("select year, permit, activity_code_1, docid as tripid, vtr_imgid from cams_GARFO.cams_subtrip       
    where activity_code_1 like 'SES%';") $myNEFSC_USERS_conn;
renvars, lower;

append using `orphans';
gen access_area=strmatch(activity_code_1,"SES-SCA-*");

save $data_main/scallop_types_$vintage_string.dta, replace;


keep permit access_area tripid;

drop if tripid==.;

compress;
save $data_main/scallop_access_area_supplement_$vintage_string.dta, replace;
 
/*
So SES-SAA is an Access Area trip by a LA vessel,
and SES-SCA is an DAS trip by a LA vessels.
And SES-SCG is a trip by a LGC vessel

*/