
#delimit;
clear;
odbc load,  exec("select * from das.das_allocation_rules where fishing_year between 2004 and 2005;") $oracle_cxn;  
rename fishery_code activity_code;

rename user_entered ue;
rename date_entered de;
rename user_c uc;
rename date_changed dc;
rename remarks activity_description;
save $my_workdir/das_activity_codes.dta, replace ;


/*AMS -- 2007 to Present */
clear;
odbc load,  exec("select * from ams.das_activity_codes;") $oracle_cxn;  
rename description activity_description;
rename das_type das_category;
save $my_workdir/ams_activity_codes.dta, replace ;



clear;
odbc load,  exec("select * from das2.valid_activity_code;") $oracle_cxn;  
save $my_workdir/das2_activity_codes.dta, replace ;

