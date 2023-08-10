#delimit;

/* Send this to character*/

clear;
odbc load,  exec("select trip_id, trip_date, docid, permit, sector_id, mult_mri, calendar_year from  APSD.T_SSB_TRIP@musky_noaa where groundfish_permit is not null;") $oracle_cxn;
save dmis_trips.dta, clear;


global firste 2007
global laste 2016


quietly forvalues yr=$firste/$laste{;
	tempfile dmis;
	local dsp1 `"`dsp1'"`dmis'" "'  ;
	clear;
	odbc load,  exec("select trip_id, trip_date, permit, sector_id, mult_mri, calendar_year from  APSD.T_SSB_TRIP@musky_noaa;") $oracle_cxn;
	renvarlab, lower;
	destring, replace;
	compress;

	gen dbyear=`yr';

	quietly save `dmis';
};



clear;
/* 
select a.*, rownum rnum 
  from (
select trip_id, stock_id, round(discard,2) as discard from APSD.T_SSB_DISCARD@musky_noaa where discard>0) a; 
global mycounter=counter[1];

*/


#delimit;
clear;
odbc load,  exec("select a.*, rownum rnum 
  from (select trip_id, stock_id, round(discard,2) as discard from APSD.T_SSB_DISCARD@musky_noaa where discard>0) a;") $oracle_cxn;
