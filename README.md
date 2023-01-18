# GeminiCalMgr

This project is for the calibration matching logic for Gemini data.  This logic
is handled via per-instrument calibration definitions.  The matching rules are 
expressed as SQLAlchemy database queries against a DRAGONS maintained sqlite
database.


## Calibration Checking

When you have a calibration file that doesn't match a target file when you
expect it to, this library provides the tool `calcheck` for inspecting the
query for mismatches to diagnose it.  It does a best-effort job but can be
helpful.  You can supply `auto` to infer the calibration type, or you can
replace `auto` below with `flat`, `bias`, etc.

  Useage: calcheck <target file> auto <calibration file>


## DRAGONS Dependencies

Unfortunately, this project still has some dependency on `DRAGONS`, which in turn 
depends on `GeminiCalMgr`.  The `DRAGONS` dependency stems from the use of 
`astrodata` to read the FITS files.  The `astrodata` objects are also used as 
inputs to the calibration logic.

Two possible long-term solutions would be to break out the `astrodata` from
`DRAGONS` as a separate library, or integrate this `GeminiCalMgr` project directly
into the DRAGONS codebase.
