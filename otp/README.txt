Steps to calculate the transit travel times:

1. Create pbf file using open trip planner (https://github.com/opentripplanner/OpenTripPlanner) (http://docs.opentripplanner.org/en/dev-2.x/Basic-Tutorial/)
2. Create graph using open trip planner (https://github.com/rafapereirabr/otp-travel-time-matrix)
3. Run the Create Census Tract Centriod jupyter notebook to create origins/destinations
4. Calculate transit travel times with python_script_loopHM_parallel.py script (https://github.com/rafapereirabr/otp-travel-time-matrix)

After input files have been downloaded and placed according to the links above... run the following notebooks and 
commands in the command prompt (must be command prompt... not power shell)


1. java -Xmx10G -jar otp-1.4.0-shaded.jar --cache C:\Workspace\TNC-Demand-Model\otp --basePath C:\Workspace\TNC-Demand-Model\otp --build C:\Workspace\TNC-Demand-Model\otp
1.1 add --inMemory to the end to create an interative graph (map)
2. Run the following jupyter notebook: C:\Workspace\TNC-Demand-Model\Create Census Tract Centroids.pynb
3. c:\jython2.7.2\bin\jython.exe -J-XX:-UseGCOverheadLimit -J-Xmx10G -Dpython.path=otp-1.4.0-shaded.jar python_script_loopHM_parallel.py


This helped me too: https://access.readthedocs.io/en/latest/resources.html 