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

### Performance

A cprofile test was completed:

#### cProfile Script (cprofile_ora_sdk.py)

```python
#!/usr/bin/env python3.8
from tap_oracle.tap import Taporacle
Taporacle.cli()
```

#### Command

```bash
python -m cProfile -o ora_sdk_sync.log cprofile_ora_sdk.py --config config_ora.json --catalog ids_catalog.json >/dev/null
```

#### Analysis

```py
import os
from pstats import Stats, SortKey
p = pstats.Stats(f"{os.getenv('HOME')}/repos/ora_sdk_sync.log")
p.strip_dirs().sort_stats(SortKey.TIME).print_stats(10)
p.strip_dirs().sort_stats(SortKey.CUMULATIVE).print_stats(10)
```

#### Output

```log
         57734168 function calls (57143219 primitive calls) in 87.235 seconds

   Ordered by: cumulative time
   List reduced from 6278 to 50 due to restriction <50>

   ncalls  tottime  percall  cumtime  percall filename:lineno(function)
    900/1    0.023    0.000   87.985   87.985 {built-in method builtins.exec}
        1    0.000    0.000   87.985   87.985 cprofile_ora_sdk.py:2(<module>)
        1    0.000    0.000   84.070   84.070 core.py:1128(__call__)
        1    0.000    0.000   84.070   84.070 core.py:987(main)
        1    0.000    0.000   84.069   84.069 core.py:1393(invoke)
        1    0.000    0.000   84.069   84.069 core.py:709(invoke)
        1    0.000    0.000   84.069   84.069 tap_base.py:461(invoke)
        1    0.001    0.001   82.094   82.094 tap.py:69(sync_all)
        1    0.411    0.411   79.824   79.824 core.py:1134(sync)
   445856    2.394    0.000   79.412    0.000 core.py:1016(_sync_records)
   445856    1.079    0.000   43.917    0.000 core.py:833(_write_record_message)
   445857    0.209    0.000   26.357    0.000 client.py:171(get_records)
   445857    3.702    0.000   26.153    0.000 sql.py:156(get_records)
   445900    0.938    0.000   23.933    0.000 messages.py:195(write_message)
   891711    2.034    0.000   18.912    0.000 client.py:242(_generate_record_messages)
   445900    0.617    0.000   18.864    0.000 messages.py:183(format_message)
   445900    1.941    0.000   17.821    0.000 __init__.py:276(dumps)
   445879    0.540    0.000   15.890    0.000 result.py:532(iterrows)
   445879    0.374    0.000   15.350    0.000 cursor.py:2089(_fetchiter_impl)
   445900    0.950    0.000   15.092    0.000 encoder.py:277(encode)
   445856    0.387    0.000   14.967    0.000 cursor.py:1234(fetchone)
      450    0.019    0.000   14.509    0.032 cursor.py:1202(_buffer_rows)
      451    0.217    0.000   14.491    0.032 cursor.py:463(fetchmany)
   446806   14.192    0.000   14.192    0.000 {method 'fetch_next_row' of 'oracledb.base_impl.BaseCursorImpl' objects}
   445900   11.941    0.000   13.841    0.000 encoder.py:306(iterencode)
       60    0.001    0.000   11.296    0.188 __init__.py:1(<module>)
   445856    4.790    0.000    5.817    0.000 _catalog.py:82(pop_deselected_record_properties)
   445856    0.848    0.000    4.731    0.000 messages.py:107(__post_init__)
   445856    0.341    0.000    4.578    0.000 _util.py:28(utc_now)
    803/5    0.006    0.000    4.525    0.905 <frozen importlib._bootstrap>:986(_find_and_load)
    794/3    0.004    0.000    4.525    1.508 <frozen importlib._bootstrap>:956(_find_and_load_unlocked)
    745/4    0.004    0.000    4.523    1.131 <frozen importlib._bootstrap>:650(_load_unlocked)
    673/4    0.002    0.000    4.523    1.131 <frozen importlib._bootstrap_external>:837(exec_module)
    968/4    0.001    0.000    4.521    1.130 <frozen importlib._bootstrap>:211(_call_with_frames_removed)
   235/26    0.001    0.000    4.335    0.167 {built-in method builtins.__import__}
   764/84    0.001    0.000    4.292    0.051 <frozen importlib._bootstrap>:1017(_handle_fromlist)
   445856    1.576    0.000    4.237    0.000 __init__.py:197(now)
   445910    4.014    0.000    4.014    0.000 {method 'flush' of '_io.TextIOWrapper' objects}
        1    0.000    0.000    3.901    3.901 tap.py:1(<module>)
   445856    0.593    0.000    3.883    0.000 datetime.py:1466(astimezone)
   445856    0.425    0.000    3.306    0.000 row.py:352(keys)
        5    0.000    0.000    3.130    0.626 core.py:1(<module>)
   445856    1.101    0.000    2.932    0.000 {function DateTime.astimezone at 0x7fbb70cb1160}
   445856    1.579    0.000    2.890    0.000 core.py:990(_process_record)
   445856    0.837    0.000    2.881    0.000 result.py:106(keys)
   445855    0.456    0.000    2.737    0.000 core.py:709(_increment_stream_state)
   445856    0.709    0.000    2.499    0.000 {built-in method now}
      673    0.010    0.000    2.447    0.004 <frozen importlib._bootstrap_external>:909(get_code)
        1    0.000    0.000    2.418    2.418 sql.py:604(get_table)
        1    0.000    0.000    2.417    2.417 sql.py:575(get_table_columns)


<pstats.Stats object at 0x7f0181b03940>
```