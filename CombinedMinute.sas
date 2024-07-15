/* Step 1: Define libraries */
libname input '/scratch/wustl/spring_research/minreturn_folder';
libname output '/scratch/wustl/spring_research';

/* Step 2: Create an empty dataset to hold the combined data */
data output.combined_minutereturns;
   set input.minutereturns_:; 
run;

