Features to implement

* Supports Full, Incremental, and Log
* Supports initial full then increment / log
* Supports connection by SID or Service
* Supports ability to work with Plugable databases
* Support for Singer Decimal notation
* Support for array size
* Support for offset for incremental (window to work within)
* Support metadata counts
* Handles some more unusual Oracle Data Types
* Logic to handle unusual Oracle DataTyping and if a type can't be assumed outputting as String (handled by SQLAlchemy, just outputting a warning):
  * SDO_DIM_ARRAY
  * SDO_GEOMETRY
  * SDO_NUMBER_ARRAY
  * SDO_STRING_ARRAY
  * XMLTYPE
* Support for selective discovery based on selected tables - discovery was previously taking several minutes
* Support for orjson, for faster json dumps
* Using thick client for backwards compatibility with the likes of Oracle 11.2
* Logic to handle databases which don't have ora_scn. ORA_SCN is used to pseduo method to order full table loads