/***********************************
Code Snippet: Inserting SAS data
into SQL (via SAS-SQL Connection)
one year-month at a time using
SAS Enterprise Guide (EG).

This code snippet is essentially
a redacted version of the code I
I wrote for a monthly finacial
"snapshot" that pulled SAS claims
data from differently-formatted
client datasets into a standardized,
multitenant SQL table.
***********************************/

/*******************************************************************
USER INPUTS: create a SAS EG project and use Prompt Manager to 
define the following variables. 

currdt  := Current month
firstdt := Start month
*******************************************************************/

/*******************************************************************
Set SAS options and DB connections
*******************************************************************/
OPTIONS THREADS SASTRACE = ',,,sa' SASTRACELOC = SASLOG NOSTSUFFIX;
/* Threads enables processor multithreading in some cases */
/* "SASTRACE..." prints # records SAS-SQL buffers at a time.
   Documentation: http://support.sas.com/kb/36/426.html */

/* turn off display of outputs/notes */
%MACRO ods_off;
    ODS EXCLUDE ALL;
    ODS NORESULTS;
    OPTIONS NONOTES;
%MEND;
 
/* re-enable display of outputs/notes */
%MACRO ods_on;
    ODS EXCLUDE NONE;
    ODS RESULTS;
    OPTIONS NOTES;
%MEND;

%ods_off

/* Pull in your SAS EG username and password */
%INCLUDE "/prg/sasutil/sqlcode/sasuser_sql_creds.sas";

/*Connect to your SQL data warehouse to create a new table to append monthly data to*/
%LET MyCXN = %CMPRES(
    DSN %BQUOTE(=) "MyDB_DSN"
    USER %BQUOTE(=) "&sasusr"
    PASSWORD %BQUOTE(=) "&saspwd"
    TRACE %BQUOTE(=) YES
    TRACEFILE %BQUOTE(=) SASLOG
    READBUFF %BQUOTE(=) 10000
    INSERTBUFF %BQUOTE(=) 10000
        /* SAS reads/inserts/updates 250/10/1 record at a time by default */
        /* documentation: http://support.sas.com/resources/papers/proceedings13/081-2013.pdf */
    );

LIBNAME MyDS SQLSVR &MyCXN.;

/*******************************************************************
Drop this month's SQL table if it has already been created.
*******************************************************************/
%PUT %CMPRES(
        (%SYSFUNC(TIME(), TIMEAMPM11.)) 
        Dropping this months SQL table if it has
        already been created%BQUOTE(.)
    );
%LET new_MyTable = prefix_&currdt._snapshot_dev;
PROC SQL NOPRINT NOERRORSTOP;
    CONNECT TO SQLSVR (&MyCXN.) ;
    EXECUTE(
            DROP TABLE [MyDB].[dbo].[&new_MyTable.];
    ) BY SQLSVR
    ;
    DISCONNECT FROM SQLSVR;
QUIT;

PROC SQL NOPRINT;
    CONNECT TO SQLSVR (&MyCXN.);
    EXECUTE(
        CREATE TABLE [MyDB].[dbo].[&new_MyTable.] (
            var1 VARCHAR(3)
            , var2 INT
            , var3 DECIMAL(10, 2)
        )
    ) BY SQLSVR
    ;
    DISCONNECT FROM SQLSVR;
QUIT;

/*******************************************************************
Pull in all applicable SAS formats
*******************************************************************/
%PUT %CMPRES(
        (%SYSFUNC(TIME(), TIMEAMPM11.)) 
        Pulling in needed SAS formats%BQUOTE(.)
    );
LIBNAME fmt "/sasprod/dw/formats";
PROC FORMAT CNTLIN=fmt.myformat; RUN;

/*******************************************************************
For each client, pull their records into the new EDW table
*******************************************************************/
%MACRO MyTable(client, clientid, iterdt);
    /*
    Client   := client to be included in MyTable
    ClientID := client's CLIENTID
    ITERDT   := Current iteration's year-month
    */
    %PUT %CMPRES(
        (%SYSFUNC(TIME(), TIMEAMPM11.)) 
        Beginning MyTable insert: Client is &client.%BQUOTE(,)
        ClientID is &clientid.%BQUOTE(,)
        Year-Month is &iterdt.%BQUOTE(.)
    );

    LIBNAME lref "/sasprod/&client./sasdata/tpa";

    PROC SQL NOPRINT /*FEEDBACK REDUCEPUT=BASE*/;
        CONNECT TO SQLSVR (&MyCXN.);
        INSERT INTO MyDS.&new_MyTable. (
            
        )
        SELECT
            "&clientid." AS var1
            , PUT(_var_x, $myformat.) AS var2
            , SUM(_var_y, _var_z) AS var3
        FROM lref.source1 AS s1
        LEFT JOIN lref.source2 AS s2
            ON s1.mid = s2.mid
                AND s1.incmonthid = s2.monthid
        WHERE s1.is_tpa = 1
            AND pdmonthid = &iterdt.
        ;
        DISCONNECT FROM SQLSVR;
    QUIT;

%MEND MyTable;

/* Loop MyTable inserts over the input months */
%MACRO iter_MyTable(client, clientid);
    /*
    Client   := client to be included in MyTable
    ClientID := client's CLIENTID
    */

    /* Modify the input start dates
       to use EDW formatting*/
    %LET start_year = %SUBSTR(%SYSFUNC(CATS(&firstdt.)), 1, 4);
    %LET start_mo = %SUBSTR(%SYSFUNC(CATS(&firstdt.)), 5);
    %LET end_year = %SUBSTR(%SYSFUNC(CATS(&currdt.)), 1, 4);
    %LET end_mo = %SUBSTR(%SYSFUNC(CATS(&currdt.)), 5);
    
    /* Generate a list of months to loop through */
    %IF &start_year. = &end_year.
    %THEN
        %DO;
            /* Loop through the current years months only */
            %IF &start_mo. > &end_mo. %THEN %PUT ERROR: Must use an earlier start date.;
            %ELSE %DO;
                %DO m = &start_mo. %TO &end_mo.;
                    %LET iter_yearmo = %SYSFUNC(CATS(&iter_year., &m.));
                    /* Add a leading 0 to the month if needed
                       (to comply with PDW/EDW yearmonth format) */
                    %IF %LENGTH(&iter_yearmo.) = 5
                    %THEN %LET iter_yearmo = %SYSFUNC(CATS(%SUBSTR(&iter_yearmo., 1, 4)
                                                           , 0, %SUBSTR(&iter_yearmo., 5)));
                    %ELSE;
                    
                    /* Run on current iterations year-month */
                    %MyTable(&client., &clientid., &iter_yearmo.)
                %END;
            %END;
        %END;
    %ELSE
        %DO;
            %IF &start_year. > &end_year. %THEN %PUT ERROR: Must use an earlier start date.;
            %ELSE %DO;
                
                %LET iter_year = &start_year.;
                
                /* First loop through input start year */
                %DO m = &start_mo. %TO 12;
                    %LET iter_yearmo = %SYSFUNC(CATS(&iter_year., &m.));
                    /* Add a leading 0 to the month if needed
                       (to comply with PDW/EDW yearmonth format) */
                    %IF %LENGTH(&iter_yearmo.) = 5
                    %THEN %LET iter_yearmo = %SYSFUNC(CATS(%SUBSTR(&iter_yearmo., 1, 4)
                                                           , 0, %SUBSTR(&iter_yearmo., 5)));
                    %ELSE;
                    
                    /* Run analysis on current iterations year-month */
                    %MyTable(&client., &clientid., &iter_yearmo.)
                %END;

                %LET iter_year = %EVAL(&iter_year. + 1);
                
                /* Loop through years between start and current years */
                %DO %WHILE (&iter_year. < &end_year.);
                    
                    %DO m = 1 %TO 12;
                        %LET iter_yearmo = %SYSFUNC(CATS(&iter_year., &m.));
                        /* Add a leading 0 to the month if needed
                           (to comply with PDW/EDW yearmonth format) */
                        %IF %LENGTH(&iter_yearmo.) = 5
                        %THEN %LET iter_yearmo = %SYSFUNC(CATS(%SUBSTR(&iter_yearmo., 1, 4)
                                                               , 0, %SUBSTR(&iter_yearmo., 5)));
                        %ELSE;
                        
                        /* Run on current iterations year-month */
                        %MyTable(&client., &clientid., &iter_yearmo.)
                    %END;

                    %LET iter_year = %EVAL(&iter_year. + 1);

                %END;
                
                /* Loop through the current year */
                %DO m = &start_mo. %TO &end_mo.;
                    %LET iter_yearmo = %SYSFUNC(CATS(&iter_year., &m.));
                    /* Add a leading 0 to the month if needed
                       (to comply with PDW/EDW yearmonth format) */
                    %IF %LENGTH(&iter_yearmo.) = 5
                    %THEN %LET iter_yearmo = %SYSFUNC(CATS(%SUBSTR(&iter_yearmo., 1, 4)
                                                           , 0, %SUBSTR(&iter_yearmo., 5)));
                    %ELSE;
                    
                    /* Run analysis on current iterations year-month */
                    %MyTable(&client., &clientid., &iter_yearmo.)
                    
                %END;
            %END;
        %END;

%MEND iter_MyTable;

%iter_MyTable(client1, 1)
%iter_MyTable(client2, 2)

%PUT %CMPRES(
        (%SYSFUNC(TIME(), TIMEAMPM11.)) 
        Indexing the final dataset%BQUOTE(.)
    );

/* Indexing enables fast subsetting on the selected variables.*/
PROC SQL;
    CONNECT TO SQLSVR (&MyCXN.);
    EXECUTE(
        CREATE INDEX index_MyTable
            ON [MyDB].[dbo].[&new_MyTable.] (var1, var2)
    ) BY SQLSVR
    ;
    DISCONNECT FROM SQLSVR;
QUIT;
/* Documentation: https://support.sas.com/resources/papers/proceedings/proceedings/sugi29/123-29.pdf */
