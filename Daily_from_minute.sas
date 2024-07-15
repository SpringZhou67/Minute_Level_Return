/* Step 1: Define the library reference */
libname scratch '/scratch/wustl/spring_research/';

/* Step 2: Filter and Sort the input dataset scratch.adjusted_minute_panel */
data scratch.adjusted_minute_panel_filtered;
    set scratch.adjusted_minute_panel (keep=sym_root date adj_iret);
run;

proc sort data=scratch.adjusted_minute_panel_filtered;
    by sym_root date;
run;

/* Step 3: Calculate daily mean returns using PROC MEANS */
proc means data=scratch.adjusted_minute_panel_filtered noprint;
    by sym_root date;
    var adj_iret;
    output out=scratch.accum_minute(drop=_type_ _freq_) mean(adj_iret)=Daily_Return;
run;

/* Step 5: Further summary statistics */
proc means data=scratch.accum_minute noprint;
    by sym_root;
    var Daily_Return;
    output out=scratch.daily_from_minute(drop=_type_ _freq_) 
           mean(Daily_Return)=Daily_Mean_Return
           std(Daily_Return)=Daily_Std_Return
           min(Daily_Return)=Daily_Min_Return
           max(Daily_Return)=Daily_Max_Return;
run;



