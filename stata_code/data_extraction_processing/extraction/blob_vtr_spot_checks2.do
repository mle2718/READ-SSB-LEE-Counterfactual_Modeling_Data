quietly do "/home/mlee/Documents/Workspace/technical folder/do file scraps/odbc_connection_macros.do"
#delimit;
clear;
macro drop _all;
set more off;
pause on;
/*MIN-yang's bit to connect to oracle and set up home directory */  
quietly do "/home/mlee/Documents/Workspace/technical folder/do file scraps/odbc_connection_macros.do";
global oracle_cxn "conn("$mysole_conn") lower";




#delimit cr
/* Pretend I have a dataset with "filename" and _n is relatively small (4M characters allowed in a macro)*/
clear

local mytrips 2200022,2281628,2282514,2284820,2286434,2324035,2432454,2432467,2660949,3294284,3294319,3343270,4295444,4327951,4332795,4334260,4338467,4342222,4360127,4360902,4361816,4713126,4720278,4721207,4723069,14810616042104,25169013081304,31047314050409,32111615083119,41041015042119
local ratsql1 "select i.docid, blob.imgid, blob.image_blob from avtr.image_scan_blob blob, images i  where i.docid in (`mytrips') and i.imgid=blob.imgid;"

#delimit ;
odbc load,  exec("`ratsql1'") dsn("cuda") user(mlee) password($mynero_pwd) lower clear;

rename docid tripid;

/*THIS IS HOW TO WRITE THE FILES
*/

quietly count;
local myobs =r(N);
local mylocation "/home/mlee/Documents/projects/Birkenbach/data_folder/images2";

capture mkdir `mylocation';

gen q=0;
quietly forvalues i=1/`myobs'{;
	local mytripid=tripid[`i'];

replace q=filewrite("`mylocation'/T_`mytripid'.tif",image_blob[`i']);
};

