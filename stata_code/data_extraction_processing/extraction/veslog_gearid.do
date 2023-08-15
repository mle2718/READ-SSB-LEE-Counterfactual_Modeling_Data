#delimit;

/* Send this to character*/

quietly forvalues yr=$firstders/$lastyr{;
	tempfile gearids;
	local dsp1 `"`dsp1'"`gearids'" "'  ;
	clear;
	noisily display "working on gearids in year `yr'" ;
	odbc load,  exec("select gearid, tripid, subtrip, serial_num, gearcode, mesh, gearqty, gearsize, nhaul,  soakhrs, soakmin, round(nvl(soakhrs,0)+ nvl(soakmin,0)/60,3) as soakhours, 
		depth , carea, tenmsq, qdsq, round(clatdeg+nvl(clatmin,0)/60+nvl(clatsec,0)/3600,8) as latitude,round(clondeg+nvl(clonmin,0)/60+nvl(clonsec,0)/3600,8) as longitude
	from vtr.veslog`yr'g g
	where g.tripid in 
		(select distinct tripid from vtr.veslog`yr't where tripcatg in('1','4'));") $oracle_cxn;
	renvarlab, lower;
	destring, replace;
	compress;

	gen dbyear=`yr';

	quietly save `gearids';
};



clear;

append using `dsp1';

	renvarlab, lower;
	destring, replace	;
	compress;

compress;

save $my_workdir/veslog_G$today_date_string.dta, replace ;

quietly forvalues yr=$firstders/$lastyr{;
	tempfile catchids;
	local dsp2 `"`dsp2'"`catchids'" "'  ;
	clear;
	noisily display "working on catches in year `yr'" ;
	odbc load,  exec("select catch_id, gearid, tripid, subtrip, sppcode, nvl(qtykept, 0) as qtykept, nvl(qtydisc,0) as qtydisc, dealnum, dealname, datesold from vtr.veslog`yr's  s
	where s.tripid in 
		(select distinct tripid from vtr.veslog`yr't where tripcatg in('1','4'));") $oracle_cxn;
	renvarlab, lower;
	destring, replace;
	compress;
	gen dbyear=`yr';
	
	quietly save `catchids';
};

clear;

append using `dsp2';

	renvarlab, lower;
	destring, replace	;
	compress;

compress;

save $my_workdir/veslog_S$today_date_string.dta, replace ;



/* Read in the tripids and corrected ports from our processed data, dro out some extraneous stuff.  I'll do a "merge update" to fill in portlnd1 and state1, treating the  processed data as correct if it exists

It may not match if either there were no landings or
use "/home/mlee/Documents/projects/spacepanels/scallop/spatial_project_10192017/veslog_species_huge.dta", clear;

*/

#delimit ;
use "$spacepanels_data", clear;
drop if tripid==.;
drop if dbyear<=2002;
keep tripid portlnd1 state1 geoid namelsad ;
dups, drop terse;
tempfile tports;
drop _expand;
save `tports', replace;



/* this is a normal data pull for years up to and including 2017 */
quietly forvalues yr=$firstders/2017{;
	tempfile tripids;
	local dsp3 `"`dsp3'"`tripids'" "'  ;
	clear;
	noisily display "working on trips in year `yr'" ;
	odbc load,  exec("select t.tripid, t.nsubtrip, t.hullnum, t.permit, t.datesail, t.tripcatg, t.crew, t.datelnd1, t.operator, t.opernum, t.portlnd1 as raw_portlnd1, t.state1 as raw_state1 from vtr.veslog`yr't  t
     where t.tripcatg in('1','4');") $oracle_cxn;
	renvarlab, lower;
	destring, replace;
	compress;
	gen dbyear=`yr';

	
	quietly save `tripids';
};

clear;
append using `dsp3';
tempfile tripids2017;
save `tripids2017', replace;


/* this patches in 2018 to end .
The matching isn't perfect. there are some trips that are joining to two entries because date_canceled from one row is equal to date_issued on the second row and the date_sail is both of them.

When this happens, you keep the 2nd of those rows.
 */

quietly forvalues yr=2018/$lastyr{;
	tempfile tripids2 ;
	local dsp4 `"`dsp4'"`tripids2'" "'  ;
	clear;
	noisily display "working on trips in year `yr'" ;
	odbc load,  exec("select t.tripid, t.nsubtrip, t.hullnum, t.permit, t.datesail, t.tripcatg, t.crew, t.datelnd1, t.operator, t.opernum, t.portlnd1 as raw_portlnd1, t.state1 as raw_state1, p.hull_id as fixed_hull_id, p.date_canceled from vtr.veslog`yr't  t
	 left join permit.vps_vessel p 
     ON t.permit=p.vp_num and 
        trunc(t.datesail)>=trunc(p.date_issued) and 
        (trunc(t.datesail)<=trunc(p.date_canceled) OR p.date_canceled is NULL)
     where t.tripcatg in('1','4') AND
       (trunc(p.date_issued)!=trunc(p.date_canceled) or p.date_canceled is null);") $oracle_cxn;
	renvarlab, lower;
	destring, replace;
	compress;
	gen dbyear=`yr';

	
	quietly save `tripids2';
};

clear;
append using `dsp4';

renvarlab, lower;
destring, replace;
compress;

bysort tripid (date_canceled): keep if _n==_N;
bysort tripid: assert _n==1;
drop hullnum;
rename fixed_hull_id hullnum

append using `tripids2017';
	
	
	
	
/*there's two trips that get filed into the wrong place */
drop if tripid==4864477 & dbyear==2015;

/* this one is actually in 2017 */
drop if tripid==4959274 & dbyear==2018;
replace datesail=clock("21mar2017 16:30:00", "DMY hms") if tripid==4959274 & dbyear==2016;

replace datelnd1=clock("22mar2017 06:00:00", "DMY hms") if tripid==4959274 & dbyear==2016;
replace dbyear=2017 if tripid==4959274 & dbyear==2016;



merge 1:1 tripid using `tports', keep(1 3);
drop _merge;
save $my_workdir/veslog_T$today_date_string.dta, replace ;

/* Alternative way to patch in hullnum  .. 
use noaa.vessels and noaa.document. It's incredibly slow to pull data from GARFO.  Somehow it's also incredibly slow to pull data from vtr.document and vtr.vessels (NEFSC copy)

I thought patching in based on the vessel_id would be best, but there are some rows where the vessel id =0 and the permit number exists.
Therefore, it's better to use the permit num to match

odbc load,  exec("select d.vessel_id, d.docid as tripid, d.date_sail, v.hull_num as hullnum, v.start_date, v.end_date, v.permit_num as gar_permit from noaa.document@garfo_nefsc d 
    LEFT JOIN noaa.vessels@garfo_nefsc v on
    d.vessel_id=v.vessel_id AND d.date_sail>=v.start_date and (d.date_sail<=v.end_date OR v.end_date is null)
	where d.vessel_id <>0;") $oracle_cxn;
	renvarlab, lower;
	destring, replace;
	compress;

	
	
	
#delimit ;
clear;
	There are alot of missings here too. 
odbc load, exec("select d.vessel_permit_num, d.vessel_id, d.docid, d.date_sail, v.hull_num, v.start_date, v.end_date, v.permit_num from vtr.document d 
    LEFT JOIN vtr.vessels v on
    d.vessel_permit_num=v.permit_num AND d.date_sail>=v.start_date and (d.date_sail<=v.end_date OR v.end_date is null);") $oracle_cxn;
	rename docid tripid;
	drop vessel_id;
	/* there are plenty of missing hullnum here too, so maybe vessels isn't the best */
save $my_workdir/document_$today_date_string.dta, replace ;
	
	
	*/
	
	
	
	
	
	
