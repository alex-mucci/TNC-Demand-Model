SOURCE: https://data.cityofchicago.org/Transportation/Transportation-Network-Providers-Trips/m6dm-c72p

The data contains ride-hailing trips with at least one trip end within the city of Chicago. I found the following rules 
apply to the data:
* When community area data is available the origin/destination was within chicago. (this is 87% of trips)

* When community area data and census tract data is available there are more than 2 trips within the 15 minute window to 
avoid data suppression. (This is 67% of trips)

* When community area data is available and census tract data is not available then the trip data is being suppressed. 
(this is rougly 20% of the trips and they will be assigned to census tracts)

* When community area data is not available and census tract data is available the origin/destination was within cook 
county but outside of Chicago city limits. (this is less than 1% of trips... they are dropped)

***** There is one correction for 348 trips that have both trip ends outside of chicago but within cook county... looks 
like there was an assignment issue with the trips near O'Hare airport... reassign trips from census tract 17031770700 
to census tract 17031980000.

* When community area data is not available and census tract data is not available the origin/destination was outside of
cook county. (roughly 12.6% of trips have one trip end with both missing... they are dropped)


The data is suppresssed to community areas when there is less than 3 trips between an OD pair within a 15 minute window.



Changes made to ridehailing data:

* trips missing a dropoff or pickup census tract traveled to/from outside of the city of Chicago... so they are dropped

* trips with $0 fare look to be either the pooled trips picked up along the way of a shared/pooled trip or canceled trips... 
need to look into this some more... 

* $0 trip total looks to be mostly private trips and maybe canceled trips... looks weird... must mean that taxes are not
built into the additional charges column


CMAP Stuff:

Privacy Aggregation Method: http://dev.cityofchicago.org/open%20data/data%20portal/2019/04/12/tnp-taxi-privacy.html 

Added trips: http://dev.cityofchicago.org/open%20data/data%20portal/2020/04/28/tnp-trips-2019-additional.html

Corrected trips: http://dev.cityofchicago.org/open%20data/data%20portal/2020/04/07/tnp-trips-2019-11.html