#delimit;
clear;
odbc load, exec("select * from sector.ace_mri_as_pounds;") $oracle_cxn;

rename year fishingyear;

/* strip off the _ACE stub*/ 
renvars *_ace, postsub("_ace" "");


merge 1:1 mri fishingyear using $my_workdir/sector_participants_$today_date_string.dta, keep(1 3);

saveold $my_workdir/psc_processed_$today_date_string.dta, replace version(12);


