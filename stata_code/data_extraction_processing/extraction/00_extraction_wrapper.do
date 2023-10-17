/*This extracts data for Anna.  There is a collection of do files that does the actual work, this is essentially a wrapper and table of contents. 
 */

#delimit ;

 
global my_projdir "C:/Users/Min-Yang.Lee/Documents/READ-SSB-Lee-Counterfactual_Modeling_Data";

global oracle_cxn " $mysole_conn";
global my_codedir "${my_projdir}/stata_code/data_extraction_processing/extraction";
global my_workdir  "${my_projdir}/data_folder/main";
global results  "${my_projdir}/results";



global spacepanels_data "C:/Users/Min-Yang.Lee/Documents/spacepanels/data_folder/main/veslog_species_huge_2023_08_09";
global income_mobility "C:/Users/Min-Yang.Lee/Documents/incomemobility/data_folder/internal/nameclean";




cd $my_codedir; 
pause off;

log using "${results}/AB_extraction.smcl", replace;
timer on 1;

local date: display %td_CCYY_NN_DD date(c(current_date), "DMY");
global today_date_string = subinstr(trim("`date'"), " " , "_", .);

global pass groundfish;


global firstyr 2004;
global secondyr =$firstyr+1;

global lastyr 2022;
global firstders 2004;

do "construct_owners.do";

do "scallop_mri.do";



do "permit_characteristics_extractions.do";

do "cfdbs_data_dump.do";

do "declaration_codes.do";

do "das_allocations_used.do";
do "das_allocations.do";


do "cr_boats.do";

#delimit ;
*do "das_allocations_used.do";
do "das_allocations_usedR.do";


do "mort_elig_criteria_extractions.do";


/* Get the sector rosters and ACE holdings */


do "roster_extractions.do";

do "psc_extractor.do";
/*
do "ace_transfers.do";
*/


/* Get VTR data at the gearid level */
do "veslog_gearid.do";
/* build the permit portfolios */

do "permit_characteristics_extractions.do";
do "fishery_key_file.do";
do "port_key_file.do";

do "dealer_key_file.do";


do "processed_data_subset.do";
do "final_geoid_clean.do";



do "copyover_operator_data.do"



log close;


/*TO DO 
local mydos: dir . files "*.do";
shell zip --password $pass -u  $my_workdir/birkenbach_code_logs_$today_date_string `mydos' metadata_*.docx AB_extraction.smcl;

cd $my_workdir;
local myc2: dir . files "*$today_date_string.dta";
 timer off 1;
timer list;

shell zip --password $pass -u birkenbach_$today_date_string `myc2' ;
 



!md5sum birkenbach_$today_date_string.zip > md5_checksum_$today_date_string;

*/

