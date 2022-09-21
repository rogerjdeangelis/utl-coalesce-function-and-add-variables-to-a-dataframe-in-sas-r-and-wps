%let pgm=utl-coalesce-function-and-add-variables-to-a-dataframe-in-sas-r-and-wps;

Coalesce function and add variables to a dataframe in sas r python and wps

github
https://tinyurl.com/5akbkja7
https://github.com/rogerjdeangelis/utl-coalesce-function-and-add-variables-to-a-dataframe-in-sas-r-and-wps

Free version of WPS can create a unlimited SAS dataset.
It can even output to the interactive sas work library

In R the mutate function allows the addition of variables to an existing dataframe

If wgtNow, current weight) is missing substitute previous weight

  Seven Solutions

        1. SAS datastep and SQL solution
        2. WPS datastep and SQL solution
        3, SAS drop down to R            (do not have IML)
        4. WPS proc R (part of WPS base - outputs sas dataset)
        5. WPS proc python (part of WPS base just copy your SQL code after pdsql(""")
        6, SAS drop down to Python (part of WPS base just copy your SQL code after pdsql(""")
           Unlike WPS you cannot create SAS datastep inside python. (same problem with SASpy Viya .../)

For setup for SAS drop down to python see

This repo
https://tinyurl.com/9x9sp9vv
https://github.com/rogerjdeangelis/utl-python-r-and-sas-sql-solutions-to-add-missing-rows-to-a-data-table

macros
https://tinyurl.com/y9nfugth
https://github.com/rogerjdeangelis/utl-macros-used-in-many-of-rogerjdeangelis-repositories
/*          _
 _ __ _   _| | ___  ___
| `__| | | | |/ _ \/ __|
| |  | |_| | |  __/\__ \
|_|   \__,_|_|\___||___/

*/                                  |  RULES
                                    |
Obs    NAME       WGTNOW    WGTPRE  |  WGTFIX
                                    |
  1    Alfred      112.5     112.5  |   112.5
  2    Alice        84.0      84.0  |    84.0

  3    Barbara        .       98.0  |    98.0  substitute 98 when wgtNow is missing

  4    Carol       102.5     102.5  |   102.5
  5    Henry       102.5     102.5  |   102.5

  6    James          .       83.0  |    83.0  substitute 83 when wgtNow is missing

  7    Jane         84.5      84.5  |    84.5
  8    Janet       112.5     112.5  |   112.5

  9    Jeffrey        .       84.0  |    84.0  substitute 93 when wgtNow is missing

 10    John         99.5      99.5  |    99.5

/*
 _                   _
(_)_ __  _ __  _   _| |_
| | `_ \| `_ \| | | | __|
| | | | | |_) | |_| | |_
|_|_| |_| .__/ \__,_|\__|
        |_|
*/

options validvarname=upcase;

libname sd1 "d:/sd1";

data sd1.have;
  set sashelp.class(obs=10 keep=name weight);
  wgtNow  = weight;
  wgtPre  = weight;
  if mod(_n_,3)=0 then wgtNow=.;
  drop weight;
run;quit;

Up to 40 obs SD1.HAVE total obs=10 20SEP2022:12:27:51

Obs    NAME       WGTNOW    WGTPRE

  1    Alfred      112.5     112.5
  2    Alice        84.0      84.0
  3    Barbara        .       98.0
  4    Carol       102.5     102.5
  5    Henry       102.5     102.5
  6    James          .       83.0
  7    Jane         84.5      84.5
  8    Janet       112.5     112.5
  9    Jeffrey        .       84.0
 10    John         99.5      99.5

/*           _               _
  ___  _   _| |_ _ __  _   _| |_
 / _ \| | | | __| `_ \| | | | __|
| (_) | |_| | |_| |_) | |_| | |_
 \___/ \__,_|\__| .__/ \__,_|\__|
                |_|
*/

Up to 40 obs WORK.WANT total obs=10 20SEP2022:12:28:21

Obs    NAME       WGTNOW    WGTPRE    WGTFIX

  1    Alfred      112.5     112.5     112.5
  2    Alice        84.0      84.0      84.0
  3    Barbara        .       98.0      98.0
  4    Carol       102.5     102.5     102.5
  5    Henry       102.5     102.5     102.5
  6    James          .       83.0      83.0
  7    Jane         84.5      84.5      84.5
  8    Janet       112.5     112.5     112.5
  9    Jeffrey        .       84.0      84.0
 10    John         99.5      99.5      99.5

/*
/ |    ___  __ _ ___
| |   / __|/ _` / __|
| |_  \__ \ (_| \__ \
|_(_) |___/\__,_|___/

*/

data want;
  set sd1.have;
  wgtFix = coalesce(wgtNow,wgtPre);
run;quit;

proc sql;
  create
     table want as
  select
     *
    ,coalesce(wgtNow,wgtPre) as wgtFix
  from
     sd1.have
;quit

/*___
|___ \    __      ___ __  ___
  __) |   \ \ /\ / / `_ \/ __|
 / __/ _   \ V  V /| |_) \__ \
|_____(_)   \_/\_/ | .__/|___/
                   |_|
*/

%utl_submit_wps64('

libname sd1 "d:/sd1";

data want;
  set sd1.have;
  wgtFix = coalesce(wgtNow,wgtPre);
run;quit;

proc print data=want;
run;quit;

proc sql;
  create
     table want as
  select
     *
    ,coalesce(wgtNow,wgtPre) as wgtFix
  from
     sd1.have
;quit;

proc print data=want;
run;quit;
');

/*____
|___ /    ___  __ _ ___   _ __
  |_ \   / __|/ _` / __| | `__|
 ___) |  \__ \ (_| \__ \ | |
|____(_) |___/\__,_|___/ |_|

*/

%utlfkil(d:/xpt/want_sasr.xpt);

%utl_submit_r64('
library(haven);
library(SASxport);
library(dplyr);
have=read_sas("d:/sd1/have.sas7bdat");
want <-have %>%
  mutate(
    WGTFIX = coalesce(WGTNOW, WGTPRE)
  );
want<- as.data.frame(want);
write.xport(want,file="d:/xpt/want_sasr.xpt");
');

libname xpt xport "d:/xpt/want_sasr.xpt";

proc print data=xpt.want;
run;quit;

/*  _
| || |    __      ___ __  ___   _ __  _ __ ___   ___   _ __
| || |_   \ \ /\ / / `_ \/ __| | `_ \| `__/ _ \ / __| | `__|
|__   _|   \ V  V /| |_) \__ \ | |_) | | | (_) | (__  | |
   |_|(_)   \_/\_/ | .__/|___/ | .__/|_|  \___/ \___| |_|
                   |_|         |_|
*/


proc datasets lib=sd1 nodetails nolist;
 delete wantwps;
run;quit;

%utl_submit_wps64('
libname wrk "d:/sd1";
proc r;
  submit;
    library(haven);
    library(dplyr);
    library(SASxport);
    have=read_sas("d:/sd1/have.sas7bdat");
    want <-have %>%
      mutate(
        WGTFIX = coalesce(WGTNOW, WGTPRE);
      );
    want;
 endsubmit;
import r=want data=wrk.wantwps;
run;quit;
proc print data=sd1.wantwps;
run;quit;
');

proc print data=sd1.wantwps;
run;quit;

/*
 ____                                                            _   _
| ___|  __      ___ __  ___   _ __  _ __ ___   ___   _ __  _   _| |_| |__   ___  _ __
|___ \  \ \ /\ / / `_ \/ __| | `_ \| `__/ _ \ / __| | `_ \| | | | __| `_ \ / _ \| `_ \
 ___) |  \ V  V /| |_) \__ \ | |_) | | | (_) | (__  | |_) | |_| | |_| | | | (_) | | | |
|____(_)  \_/\_/ | .__/|___/ | .__/|_|  \___/ \___| | .__/ \__, |\__|_| |_|\___/|_| |_|
                 |_|         |_|                    |_|    |___/
*/

%utl_submit_wps64('
proc python;
  submit;
    from os import path;
    import pandas as pd;
    import xport;
    import xport.v56;
    import pyreadstat;
    import numpy as np;
    import pandas as pd;
    from pandasql import sqldf;
    mysql = lambda q: sqldf(q, globals());
    from pandasql import PandaSQL;
    pdsql = PandaSQL(persist=True);
    sqlite3conn = next(pdsql.conn.gen).connection.connection;
    sqlite3conn.enable_load_extension(True);
    sqlite3conn.load_extension("c:/temp/libsqlitefunctions.dll");
    mysql = lambda q: sqldf(q, globals());
    have, meta = pyreadstat.read_sas7bdat("d:/sd1/have.sas7bdat");
    print(have);
    res = pdsql("""
     select
        *
       ,coalesce(wgtNow,wgtPre) as wgtFix
     from
        have
      ;""");
    print(res);
    endsubmit;
    libname sd1 "d:/sd1";
    IMPORT DATA=sd1.want_res PYTHON=res;
    run;quit;
  ');
libname sd1 "d:/sd1";
proc print data=sd1.want_res;
title " From Python &sysdate";
run;quit;

/*__                                  _   _
 / /_     ___  __ _ ___   _ __  _   _| |_| |__   ___  _ __
| `_ \   / __|/ _` / __| | `_ \| | | | __| `_ \ / _ \| `_ \
| (_) |  \__ \ (_| \__ \ | |_) | |_| | |_| | | | (_) | | | |
 \___(_) |___/\__,_|___/ | .__/ \__, |\__|_| |_|\___/|_| |_|
                         |_|    |___/
*/

* macros needed for drop down to python;
%macro utl_pybegin39;

   %utlfkil(c:/temp/py_pgm.py);
   %utlfkil(c:/temp/py_pgm.log);
   %utlfkil(c:/temp/example.xpt);
   filename ft15f001 "c:/temp/py_pgm.py";

%mend utl_pybegin39;

%macro utl_pyend39;
   run;quit;

   * EXECUTE THE PYTHON PROGRAM;
   options noxwait noxsync;
   filename rut pipe  "d:\Python310\python.exe c:/temp/py_pgm.py 2> c:/temp/py_pgm.log";
   run;quit;

   data _null_;
     file print;
     infile rut;
     input;
     put _infile_;
     putlog _infile_;
   run;quit;

   data _null_;
     infile "c:/temp/py_pgm.log";
     input;
     putlog _infile_;
   run;quit;

%mend utl_pyend39;

/*                           _       _
 _ __  _   _   ___  ___ _ __(_)_ __ | |_
| `_ \| | | | / __|/ __| `__| | `_ \| __|
| |_) | |_| | \__ \ (__| |  | | |_) | |_
| .__/ \__, | |___/\___|_|  |_| .__/ \__|
|_|    |___/                  |_|
*/


proc datasets lib=work kill;
run;quit;

%utlfkil(c:/temp/want.xpt);

%utl_pybegin39;
parmcards4;
from os import path
import pandas as pd
import xport
import xport.v56
import pyreadstat
import numpy as np
import pandas as pd
from pandasql import sqldf
mysql = lambda q: sqldf(q, globals())
from pandasql import PandaSQL
pdsql = PandaSQL(persist=True)
sqlite3conn = next(pdsql.conn.gen).connection.connection
sqlite3conn.enable_load_extension(True)
sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll')
mysql = lambda q: sqldf(q, globals())
have, meta = pyreadstat.read_sas7bdat("d:/sd1/have.sas7bdat")
print(have);
res = pdsql("""
select
   *
  ,coalesce(wgtNow,wgtPre) as wgtFix
from
   have
    ;""")
print(res);
ds = xport.Dataset(res, name='want_py')
with open('c:/temp/want.xpt', 'wb') as f:
    xport.v56.dump(ds, f)
;;;;
%utl_pyend39;

libname pyxpt xport "c:/temp/want.xpt";

proc print data=pyxpt.want_py;
title "  dataset want_py";
run;quit;

dataset want_py

   Obs    NAME       WGTNOW    WGTPRE    WGTFIX

     1    Alfred      112.5     112.5     112.5
     2    Alice        84.0      84.0      84.0
     3    Barbara        .       98.0      98.0
     4    Carol       102.5     102.5     102.5
     5    Henry       102.5     102.5     102.5
     6    James          .       83.0      83.0
     7    Jane         84.5      84.5      84.5
     8    Janet       112.5     112.5     112.5
     9    Jeffrey        .       84.0      84.0
    10    John         99.5      99.5      99.5

/*              _
  ___ _ __   __| |
 / _ \ `_ \ / _` |
|  __/ | | | (_| |
 \___|_| |_|\__,_|

*/
