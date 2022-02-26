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
