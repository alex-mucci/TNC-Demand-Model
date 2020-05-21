SystemName=King Co. Metro/Sound Transit
RunName=Madison Streetcar
AccessTypeLow=1
AccessTypeHigh=3
BandBufferDistance=1.2
GTFRootSuffix= 
GTFRoot=C:\Workspace\TNC-Demand-Model\STOPS\inputs\KCMBLD\
  GTFCalendarDay=20130213
  RouteIDBegCol=1
  RouteIDEndCol=9
  StopIDBegCol=1
  StopIDEndCol=9
  TripIDBegCol=1
  TripIDEndCol=9
GTFRootSuffix= 
GTFRoot=C:\Workspace\TNC-Demand-Model\STOPS\inputs\SND\
  GTFCalendarDay=20130213
  RouteIDBegCol=1
  RouteIDEndCol=100
  StopIDBegCol=1
  StopIDEndCol=100
  TripIDBegCol=1
  TripIDEndCol=100
STOP_Project_Penalty=C:\Workspace\TNC-Demand-Model\STOPS\outputdata\STOP_Project_Penalty.txt
OutputPath=True
PrintICentroid=53061   537 
PrintJCentroid=53061   537 
PrintICentroid=53033    25 
PrintJCentroid=53033    25 
PrintICentroid=53033    94 
PrintJCentroid=53033    94 
PrintICentroid=53033   136 
PrintJCentroid=53033   136 
PrintICentroid=53033   164 
PrintJCentroid=53033   164 
PrintICentroid=53033   318 
PrintJCentroid=53033   318 
PrintICentroid=53033   366 
PrintJCentroid=53033   366 
PrintICentroid=53033   530 
PrintJCentroid=53033   530 
PrintICentroid=53033   411 
PrintJCentroid=53033   411 
PrintICentroid=53053   712 
PrintJCentroid=53053   712 
PathName=2-OffPeak TR
StartHR=12
StartMIN=30
HWStartHR=12
HWStartMIN=30
ArrivalBasedS2S=True
ArrivalHR=13
ArrivalMIN=00
nTimeSamples=6
SampleTimeRange=60
IVTTMode0=1.00
IVTTMode1=1.00
IVTTMode2=1.00
IVTTMode3=1.0
IVTTMode4=1.00
IVTTMode5=1.00
IVTTMode6=1.00
IVTTMode7=1.00
IVTTMode8=1.00
OutputRoot=C:\Workspace\TNC-Demand-Model\STOPS\Skims\TZ_KCMBLD-SND_STOPS_Path_OP_TR_BLD-
WalkCost=1.0 
PEFBreak1=    100.00
PEFFactor1=      1.10
PEFBreak2=    130.00
PEFFactor2=      1.10
PEFBreak3=    160.00
PEFFactor3=      1.10
PEFBreak4=    999.00
PEFFactor4=      1.10
BoardCost=5.0 
WaitCost=1.00
Wait2Cost=1.00
WaitMinimum=0.0 
IVTTCost=1.0
FGBus_Cost_Adjustment=-5.0
MaxTripMIN=180
WalkLimit=1.0
XferLimit=0.25
WalkSpeed=3.00
WalkConstant=0.8
runone=false
person_trips=false
tersepaths=false
stoptm_tolerance=0
stop_tolerance=0
debug=false
pathtype=time-cost
choicetimefactor=1.
choicecostfactor=3.
actual-avgwaitcostfactor=0.33
KNRSpeed=25.0
KNRLimit=3.0
KNRConstant=1.0
KNRCost=1.5
PNRSpeed=25.0
PNRLimit(1)=25.0
PNRLimit(2)=10.0
PNRLimit(3)=6.0
PNRLimit(4)=3.0
PNRConstant=1.0
PNRCost=1.5
StartTimeMargin=5
StartTimeMarginCost=0
WaitCalcType=StartTime
ExportRoot=GTFExportSTOPS
ImportRoot=GTFImportSTOPS
TAZRoot=C:\Workspace\TNC-Demand-Model\STOPS\OutputData\TZ_
ExportAccessLinks=false
ImportAccessLinks=false
!ExportOnly=true
ExportEstimateOnly=True
WalkTolerance=0.2
XferTolerance=0.1
KNRTolerance=0.0
PNRTolerance=0.0
MaxExportRecords=10000
GTFExport=true
GTFExportRoot=C:\Workspace\TNC-Demand-Model\STOPS\GTFOutput\OP_TR_BLD-\
