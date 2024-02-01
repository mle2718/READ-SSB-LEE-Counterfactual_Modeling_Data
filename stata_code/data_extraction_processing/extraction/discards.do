#delimit;

/* Send this to character

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

  
  APSD_T_SSB_CATCH_CURRENT
APSD_T_SSB_DISCARD_CURRENT
APSD_T_SSB_TRIP_CURRENT
  
  */
  
clear;
odbc load,  exec("select D.permit, D.trip_id as dmis_trip_id, D.stock_id, sum(D.discard) as discard, T.docid as tripid from NEFSC_GARFO.APSD_T_SSB_DISCARD_CURRENT D, NEFSC_GARFO.APSD_T_SSB_TRIP_CURRENT T
 where D.trip_id=T.trip_id  and D.DISCARD>0 
  group by D.permit, D.trip_id, D.stock_id, T.docid;") $myNEFSC_USERS_conn;
bysort dmis_trip_id stock_id: assert _N==1;
save $data_main/dmis_discards_$vintage_string.dta, replace;
