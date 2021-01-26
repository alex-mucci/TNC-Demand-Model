Steps to calculate the transit travel times:

1. Create pbf file using open trip planner (https://github.com/opentripplanner/OpenTripPlanner) (http://docs.opentripplanner.org/en/dev-2.x/Basic-Tutorial/)
	https://download.geofabrik.de/north-america/us/illinois.html		http://docs.opentripplanner.org/en/latest/Preparing-OSM/#cropping-osm-data
2. Create graph using open trip planner (https://github.com/rafapereirabr/otp-travel-time-matrix)
3. Run the Create Census Tract Centriod jupyter notebook to create origins/destinations
4. Calculate transit travel times with python_script_loopHM_parallel.py script (https://github.com/rafapereirabr/otp-travel-time-matrix)

After input files have been downloaded and placed according to the links above and 
a folder named as your city (ex. Chicago) has been made... run the following notebooks and 
commands in the command prompt (must be command prompt... not power shell)

Adding the OSM files was not straightforward:
Download illinois statewide pbf file from - https://download.geofabrik.de/
Download osmosis from - https://github.com/openstreetmap/osmosis/releases/tag/0.48.3
Add osmosis/bin to PATH variable
Run the following command where the pbf file is to filter the file to only chicago - 
	osmosis --rb illinois-latest.osm.pbf --bounding-box left=-87.9401 right=-87.524 bottom=41.6439 top=42.023 --wb chicago.osm.pbf

*** Make sure the 64 bit java version is downloaded

1. java -Xmx10G -jar otp-1.4.0-shaded.jar --cache D:\TNC-Demand-Model\otp --basePath D:\TNC-Demand-Model\otp --build D:\TNC-Demand-Model\otp
1.1 add --inMemory to the end to create an interative graph (map)
2. Run the following jupyter notebook: C:\Workspace\TNC-Demand-Model\Create Census Tract Centroids.pynb
3. c:\jython2.7.2\bin\jython.exe -J-XX:-UseGCOverheadLimit -J-Xmx10G -Dpython.path=otp-1.4.0-shaded.jar python_script_loopHM_parallel.py

Make a folder named Graphs to contain the graphs for each of the GTFS combinations. There 
are 6 in total.

This helped me too: https://access.readthedocs.io/en/latest/resources.html 