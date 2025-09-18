/* ================== CONFIG ================== */ 
%let HORIZON = 5;

%macro _ensure(var, def);
  %if %symexist(&var.) = 0 or %superq(&var.) = %then %let &var.=&def.;
%mend;
%_ensure(HORIZON, 5);

/* -------- Forecast with PROC ESM (damped trend) -------- */
proc sort data=work.daily; by date_sas; run;

proc esm data=work.daily outfor=work.sent_forecast lead=&HORIZON out=_null_ print=all;
  forecast sentiment_index / model=damptrend transform=None;
run;

/*--Return forcasting values--*/
data work.to_write;
  set work.sent_forecast;
run;