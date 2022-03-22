


import pandas as pd
import numpy as np
import os
import pickle
import obspy
from subprocess import call
import json

def events_data_frame(working_direc, fname_cat, export_DF_path):

    '''
    This function convert catalog.xml to a data frame.
    The "events.pkl" exported to export_DF_path.

    Important Note: Reading xml file is computationally expensive.
    Try to use this function once and use to "events.pkl" instead of catalog.xml.
    '''
    # Reading the catalog.xml file
    fname_out = os.path.join(working_direc, fname_cat)

    # Creating catalog.json file in the working directory
    json_name = fname_out.replace(fname_cat, 'catalog.json')
    catalog = obspy.read_events(fname_out)
    catalog.write(json_name, format="JSON") 

    # Reading json file
    f = open(json_name)
    picks_json = json.load(f)

    # Extract information from json file and store in the events
    x= pd.DataFrame(picks_json.items(), columns=['event', 'ID']).explode('ID')

    events = pd.json_normalize(json.loads(x.to_json(orient="records")))
    events = events.filter(['ID.picks', 'ID.origins', 'ID.magnitudes'])

    events = events.explode("ID.picks")
    events = events.explode("ID.origins")
    events = events.explode("ID.magnitudes")
    events= pd.json_normalize(json.loads(events.to_json(orient="records")))
    events = events.filter(['ID.picks.time','ID.picks.time_errors.uncertainty',
            'ID.picks.waveform_id.network_code', 'ID.picks.waveform_id.station_code',
            'ID.picks.phase_hint', 'ID.origins.time', 'ID.origins.longitude','ID.origins.latitude',
            'ID.magnitudes.mag'])
    
    events= events.rename(columns={"ID.picks.time": "picks_time", "ID.picks.time_errors.uncertainty": "picks_uncertainty",
            'ID.picks.waveform_id.network_code':'network_code', 'ID.picks.waveform_id.station_code':'station_code',
            'ID.picks.phase_hint':'phase_hint', 'ID.origins.time':'origins_time', 'ID.origins.longitude':'origins_longitude',
            'ID.origins.latitude':'origins_latitude','ID.magnitudes.mag':'magnitudes'})

    file_name = '2020_events.pkl'
    events.to_pickle(os.path.join(export_DF_path , file_name))



if __name__ == "__main__":


    working_direc = '/home/javak/Sample_data_chile'

    # File name
    fname_cat = "2020.xml"
    export_DF_path = '/home/javak/Sample_data_chile/Comparing PhaseNet and Catalog'
