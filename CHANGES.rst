1.1.20

calibration
^^^^^^^^^^^

- Include camera descriptor to support GHOST calibration rules

1.1.19
======

calibration_ghost
^^^^^^^^^^^^^^^^^

- Initial calibration class for GHOST


1.1.12
======

calrequestlib
^^^^^^^^^^^^^

- building composite detector_binning field so we can query it against the parsed Header field of same name

calibration_*
^^^^^^^^^^^^^

- removing all applicables for bpms

calibration_f2
^^^^^^^^^^^^^^

- read_mode not needed to match for arcs and flats (checked with Joan)

associate_calibrations
^^^^^^^^^^^^^^^^^^^^^^

- do final sort to bubble up any BPMs to the top of the list

debugging
^^^^^^^^^

- improved support for Group query elements

calibration_x
^^^^^^^^^^^^^

- support for all instruments for calibration debugging
- support for BPMs


1.1.5
=====

calcheck
^^^^^^^^

- fixes to the command line tool for checking calibrations


1.1.4
=====

calibrations_*
^^^^^^^^^^^^^^

- support for QH* gcal lamps


1.1.3
=====

Other
-----

ghost_calmgr
^^^^^^^^^^^^

- Keying off reduction type PROCESSED_UNKNOWN as that is how the reduction is set in the Header table


1.1.2
=====

Other
-----

calibrations
^^^^^^^^^^^^

- using literal_eval for types parsing to be safe from malicious data [#2]


1.1.0
=====

User-Facing Changes
-------------------

gmos_calmgr
^^^^^^^^^^^

- Initial version now decoupled from the FitsStorage codebase
