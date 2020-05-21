import arcpy
mxd = arcpy.mapping.MapDocument(r"C:\Workspace\TNC-Demand-Model\STOPS\Chicago\StationTemplate.mxd")
df = arcpy.mapping.ListDataFrames(mxd, "*")[0]
#stops=arcpy.mapping.ListLayers(mxd, "STOPSStations", data_frame=df)
stops=arcpy.mapping.ListLayers(mxd, "STOPSStations", data_frame=df)[0]
arcpy.SelectLayerByAttribute_management(stops,"NEW_SELECTION",'"STOPSTYPE" > 0')
df.extent = stops.getSelectedExtent(True)
df.scale *= 0.8
arcpy.RefreshActiveView()
arcpy.SelectLayerByAttribute_management(stops,"CLEAR_SELECTION")
arcpy.RefreshActiveView()
mxd.saveACopy(r"C:\Workspace\TNC-Demand-Model\STOPS\Chicago\Station.mxd")
del mxd
