#!/bin/csh
set yr=2012
set outfile="usarray_tele${yr}.txt"
#rm $outfile
set dbdir="../anf"
foreach mon (01 02 03 04 05 06 07 08 09 10 11 12)
  set dbname=usarray_${yr}_${mon}
  set dbpath=${dbdir}/events_${dbname}/$dbname
  echo Working on database $dbname
  dbjoin ${dbpath}.event origin \
  | dbsubset - "orid==prefor" \
  | dbjoin - netmag \
  | dbsubset - "magnitude>5.0" \
  | dbjoin - assoc arrival \
  | dbsubset - "assoc.delta>30.0 && assoc.delta<95.0" \
  | dbselect - event.evid origin.lat origin.lon origin.depth origin.time origin.mb origin.ms assoc.sta assoc.phase arrival.iphase assoc.delta assoc.seaz assoc.esaz assoc.timeres arrival.time arrival.deltim \
  >> $outfile
end
