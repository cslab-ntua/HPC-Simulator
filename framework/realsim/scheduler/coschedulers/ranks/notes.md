# Notes on backfill algorithms in co-scheduling

- A job block at the front of the waiting queue will either demand to be
co-allocated for an already executing unit or to get space for a co-scheduled or
compact execution.
- If the blocked job is to be executed as *compact* then there are two options.
  Either find a place inside an already executing unit or wait until enough
  space for a full-node-exclusive compact allocation is available.

  For jobs that can fit inside current executing units and will finish earlier
  than the co-location estimated time 
