import pandas as pd
import numpy as np
import folium
import geopandas as gp
    
def visualize_suppressed_trips(grouped, tracts_shapefile_path, tract_centroids_file_path, outfile):     
    print('VISUALIZING SUPPRESSED TRIPS!')
    grouped['TOTAL_SUP_TRIPS'] = grouped.SUP_PRIVATE_TRIPS + grouped.SUP_SHARED_TRIPS
    origin = grouped[['GEOID_PICKUP', 'TOTAL_SUP_TRIPS']].groupby(by = 'GEOID_PICKUP', as_index = False).sum()
    dest = grouped[['GEOID_DROPOFF', 'TOTAL_SUP_TRIPS']].groupby(by = 'GEOID_DROPOFF', as_index = False).sum()
    geo = gp.read_file(tracts_shapefile_path)
    
    origin['GEOID_PICKUP'] = origin.GEOID_PICKUP.astype(float)
    dest['GEOID_DROPOFF'] = dest.GEOID_DROPOFF.astype(float)
    geo['geoid10'] = geo.geoid10.astype(float)
    centroids = pd.read_csv(tract_centroids_file_path)
    centroids = gp.GeoDataFrame(centroids)
    
    bins = np.quantile(origin['TOTAL_SUP_TRIPS'], [0,0.5,0.75,0.9,0.98,1])

    m = folium.Map([41.8781, -87.6298], zoom_start=11)
    
    
    # Add the color for the chloropleth:
    folium.Choropleth(
     geo_data=geo,
     name= "Suppressed Pickups",
     data=origin,
     columns = ['GEOID_PICKUP', 'TOTAL_SUP_TRIPS'],
     key_on='feature.properties.geoid10',
     fill_color='BuGn',
     fill_opacity=0.6,
     line_opacity=0.2,
     bins = bins,
     legend_name='Average Weekday Pickups',
     highlight = True).add_to(m)

    folium.Choropleth(
     geo_data=geo,
     name= "Suppressed Dropoffs",
     data=dest,
     columns = ['GEOID_DROPOFF', 'TOTAL_SUP_TRIPS'],
     key_on='feature.properties.geoid10',
     fill_color='BuGn',
     fill_opacity=0.6,
     line_opacity=0.2,
     bins = bins,
     legend_name='Average Weekday Dropoffs',
     highlight = True).add_to(m)

    feature_group = folium.FeatureGroup(name='Census Tract Centroids', show = False)


    for tract2 in centroids.GEOID:
        row = centroids[centroids['GEOID'] == tract2]
        folium.CircleMarker([row['Y'], row['X']], popup = str(int(row['GEOID'].values[0])), radius = 1, fill = True, fill_color = 'grey', color = 'grey').add_to(feature_group)

    m.add_child(feature_group)


    folium.LayerControl().add_to(m)

    m.save(outfile)

    print('SUPPRESSED TRIPS ARE VISUALIZED!')