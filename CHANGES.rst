1.1.12
======

debugging
^^^^^^^^^

- improved support for Group query elements

calibration_nifs
^^^^^^^^^^^^^^^^

- support for NIFS calibration debugging


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
