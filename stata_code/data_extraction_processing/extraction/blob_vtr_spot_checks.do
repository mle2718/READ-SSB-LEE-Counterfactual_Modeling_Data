#delimit;
clear;
macro drop _all;
set more off;
pause on;
/*MIN-yang's bit to connect to oracle and set up home directory */  




#delimit cr
/* Pretend I have a dataset with "filename" and _n is relatively small (4M characters allowed in a macro)*/
clear

local mytrips 3170091,2328430,2448146,2334436,3051612,2325260,2326874,2345775,2364532,2328275,2827712,2313945,2474680,2474677,2342019,2801846,2753122,2518767,2518769,2323199,2325584,2344005,2309994,2724383,2458654,2431894,2786537,2416603,2401980,3226764,2469160,3170931,3195262,3195258
local ratsql1 "select i.docid, blob.imgid, blob.image_blob from avtr.image_scan_blob blob, images i  where i.docid in (`mytrips') and i.imgid=blob.imgid;"

#delimit ;
odbc load,  exec("`ratsql1'") dsn("cuda") user(mlee) password($mynero_pwd) lower clear;

rename docid tripid;

/*THIS IS HOW TO WRITE THE FILES
*/

quietly count;
local myobs =r(N);
local mylocation "${my_workdir}/images";

capture mkdir `mylocation';

gen q=0;
quietly forvalues i=1/`myobs'{;
	local mytripid=tripid[`i'];

replace q=filewrite("`mylocation'/T_`mytripid'.tif",image_blob[`i']);
};

