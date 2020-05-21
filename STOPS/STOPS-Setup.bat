md datatemplates
md stopscomponents

copy stopspackage.dat *.exe
del stopsmenu.exe
stopspackage
copy STOPSLineData*.* datatemplates\*.*
del STOPSLineData*.*
copy STOPSStations*.* datatemplates\*.*
del STOPSStations*.*
copy DistrictTemplate*.* datatemplates\*.*
del DistrictTemplate*.*
copy StationTemplate*.* datatemplates\*.*
del StationTemplate*.*
copy TimeTripsTemplate*.* datatemplates\*.*
del TimeTripsTemplate*.*

copy *.exe stopscomponents\*.*
del *.exe
copy stopscomponents\stopsmenu.exe
copy stopscomponents\stopsmenu.exe
copy *.1 stopscomponents\*.*
del *.1
copy *.2 stopscomponents\*.*
del *.2
copy *.3 stopscomponents\*.*
del *.3
copy *.4 stopscomponents\*.*
del *.4
copy *.5 stopscomponents\*.*
del *.5
copy *.6 stopscomponents\*.*
del *.6
copy *.dbd stopscomponents\*.*
del *.dbd