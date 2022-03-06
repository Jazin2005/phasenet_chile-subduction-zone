import pandas as pd
import csv
import numpy as np  
import obspy
import json
import os
import matplotlib.pyplot as plt
from subprocess import call
import pickle



class Waves_Picker(object):

    def __init__(self, phasenet_traj:'str', working_traj:'str', export_fig_path:'str', 
                run_PhaseNet:'bool', dt:'int', starttime, fname_cat:'str', fname_inv:'str',
                events_DF:'bool',station_name_list:'str'):

        '''
        Parameters initialization
            - phasenet_traj: The trajectory of PhaseNet.
            - working_traj: The trajectory of working space.
                        This trajectory must have the following items:
                            - mseed folder: contains all mseed data for prediction.
                            - statlist.txt: contains desired stations sotred in txt files.
                            - mseed.csv: The list of data existed in mseed folder.
                            - catalog.xml: the xml file of catalog
                            - stations.xml: the xml of all stations
            - export_fig_path: The trajectory of output files
            
            - run_PhaseNet (bool): This is a bool variable:
                            - True: Run PhaseNet and plot the result of PhaseNet and Catalog
                            - False: Just plot the result of PhaseNet and Catalog based on
                                    the 'result_PhaseNet.pkl'
            - dt(int): delta time, ms. example: 7000
            - starttime: start time. example: obspy.UTCDateTime("2020-12-31T08:19:57.480000Z")

            - fname_cat: The name of catalog (catalog must be xml file). example:catalog.xml
            - fname_inv: The name Stations (Stations must be xml). example:stations.xml
            - events_DF (bool): This is a bool variable:
                            - True: Convert catalog.xml to data frame and export "events.pkl"
                            - False: using current "events.pkl" located in "export_fig_path"
            - station_name_list: the name of selected station stored in the text file


        '''
        os.chdir('{0}'.format(phasenet_traj))
        self.PROJECT_ROOT = os.getcwd()
        self.working_traj = working_traj
        self.export_fig_path = export_fig_path
        self.run_PhaseNet= run_PhaseNet
        self.dt = dt
        self.starttime = starttime
        self.fname_inv = fname_inv
        self.fname_cat= fname_cat
        self.events_DF = events_DF
        self.station_name_list=station_name_list


    def __call__(self):

        '''
        This function run PhaseNet and plot the result of PhaseNet
        '''       
        
        # Read the station names from text file and extract file name
        stations = self.get_stations ()

        # Sort stations based on latitude
        stations = self.sort_stations_latitude(stations)

        file_name = self.extract_data_name()

        # sort file name based on the lat. or long.
        file_name  = self.sort_file_name (file_name, stations)

        if self.run_PhaseNet==True: 
            # Creates pandas DataFrame.
            data = {'P_waves':[],'S_waves':[]}
            df = pd.DataFrame(data, index =[])  

            for i in file_name:
                daily_data = i
                print(daily_data)

                # write the name of daily data in scv
                self.write_mseed_names(daily_data)

                # Run PhaseNet based on the given day
                self.waves_picking ()

                # Pick P-waves and S-waves
                df_p_waves, df_s_waves = self.read_picks ()

                # Perform slicing based on starttime and dt
                #df_p_waves = self.waves_slicing(df_p_waves, self.starttime, self.dt)
                #df_s_waves = self.waves_slicing(df_s_waves, self.starttime, self.dt)


                # save data in data frame
                df_total = self.save_DF (df_p_waves, df_s_waves, daily_data, df)
                df = df_total

                print('-----------------------------------')
                print(df.shape)
                print('-----------------------------------')
            
            # save file in the export directory
            file_name = 'result_PhaseNet.pkl'
            df.to_pickle(os.path.join(self.export_fig_path, file_name))

            if self.events_DF == True:
                self.events_data_frame()
                # Plot the PhaseNet result
                self.plotting()
            else:
                # Plot the PhaseNet result
                self.plotting()
        
        else:
            if self.events_DF == True:
                self.events_data_frame()
                # Plot the PhaseNet result
                self.plotting()
            else:
                # Plot the PhaseNet result
                self.plotting()

    def save_DF (self, df_p_waves, df_s_waves, daily_data, df):

        data = {'P_waves':[df_p_waves],
                'S_waves':[df_s_waves]
                }

        # Creates pandas DataFrame.
        df_new = pd.DataFrame(data, index =[daily_data])
        df_total = df.append(df_new)
        return df_total
    
    def sort_file_name (self, file_name, stations):

        stations = stations["station"]
        sorted_stations=[]

        for i in range (0, stations.shape[0]):
            for j in range (0, len(file_name)):
                if file_name[j][3:7] == stations.iloc[i]:
                    sorted_stations.append(file_name[j])
        
        return sorted_stations



    
    def get_stations (self):
        '''
        Return the list of stations stored in station_name_list
        
        Return:
            - stations (list): The list of stations
        '''
        stations = pd.read_csv(os.path.join(self.working_traj, self.station_name_list), sep="\t")
        return stations
    
    def write_mseed_names(self, mseed_name):
        '''
    
        '''
        df = pd.DataFrame ([mseed_name], columns = ['fname'])

        df.to_csv((os.path.join(self.working_traj, 'mseed.csv')),index=False)
    
    def waves_picking (self):
        '''
        Run the predefined PhaseNet model (model/190703-214543) to pick S and P waves.
        '''
        cmd = '/home/javak/miniconda3/envs/phasenet/bin/python phasenet/predict.py --model=model/190703-214543 --data_list=/home/javak/Sample_data_chile/mseed.csv --data_dir=/home/javak/Sample_data_chile/mseed --format=mseed --plot_figure'.split()
        call(cmd)
    
    def extract_data_name(self):
        '''
        Return the mseed files names stored in mseed folder
        '''
        for file_name in os.walk(os.path.join(self.working_traj, 'mseed')):
            pass
        return file_name[2]
    
    def compare_data_station (self):

        stations = self.get_stations (self.station_name_list)
        stations = self.sort_stations_latitude(stations)

        #daily_data_name = self.extract_data_name()
        #for i in range (0, len(daily_data_name)):
        #    if daily_data_name[i][0:2] != network:
        #t = [e for e in daily_data_name if e[0][0:2] in network]
        #x = daily_data_name[0][0:7]
        #y = len(daily_data_name)

        return stations

    
    def sort_stations_latitude(self,stations):
        '''
        Sort station based on latidude.
            Parameters:
                        stations (DataFrame): This data frame contains at least two culomns (station name and corespondinglatidude)
        '''

        return stations.sort_values("latidude", ascending=False)
    
    
    def sort_stations_longitude(self,stations):

        '''
        Sort station based on longitude.
            Parameters:
                        stations (DataFrame): This data frame contains at least two culomns (station name and coresponding longitude)
        '''

        return stations.sort_values("longitude", ascending=False)


    def read_picks (self):
        '''
        Read the csv file of PhaseNet output and return the P waves and S waves.
        '''
        picks_csv = pd.read_csv(os.path.join(self.PROJECT_ROOT, "results/picks.csv"), sep="\t")
        picks_csv.loc[:, 'p_idx'] = picks_csv["p_idx"].apply(lambda x: x.strip("[]").split(","))
        picks_csv.loc[:, 'p_prob'] = picks_csv["p_prob"].apply(lambda x: x.strip("[]").split(","))
        picks_csv.loc[:, 's_idx'] = picks_csv["s_idx"].apply(lambda x: x.strip("[]").split(","))
        picks_csv.loc[:, 's_prob'] = picks_csv["s_prob"].apply(lambda x: x.strip("[]").split(","))

        with open(os.path.join(self.PROJECT_ROOT, "results/picks.json")) as fp:
            picks_json = json.load(fp)
        
        df = pd.DataFrame.from_dict(pd.json_normalize(picks_json), orient='columns')
        df_p_waves = df[df["type"] == 'p']
        df_s_waves = df[df["type"] == 's']

        return df_p_waves, df_s_waves
    
    def waves_slicing(self,waves, starttime, dt):
        '''
        Perform Slicing on P waves or S waves.

            Parameters:
                        - starttime (str): start time for slicing (like 2020-12-31T12:30:58.180000Z)
                        - dt (int): time interval for sclicing
                        - waves (DataFrame): This is a data frame (PhaseNet output) contains "id", "timestamp", "prob", and "type".
                                            This data frame must contains "s" or "p" type not both of them.
            
            return:
                    - new_df_waves: sliced data frame according to "starttime" and "dt"

        '''
        mask = (waves['timestamp']> starttime) & (waves['timestamp']<starttime+dt)
        new_df_waves = waves.loc[mask]
        print(new_df_waves)
        return new_df_waves
    
    def read_data (self, daily_data):
        '''
        Read the mseed daily data and return stream.
            Parameters:
                - daily_data (str): The name of daily mseed file ( like CX.PB06..HHZ.D.2020.366)
            return:
                    - stream: obspy stream data
        '''
        stream = obspy.read(os.path.join(self.working_traj, 'mseed', '{0}'.format(daily_data)), sep="\t")
        return stream
    
    def data_slicing (self, starttime, dt, daily_data):
        '''
        Perform Slicing on stream.
            Parameters:
                        - starttime (str): start time for slicing (like 2020-12-31T12:30:58.180000Z)
                        - dt (int): time interval for sclicing
                        - daily_data (str): The name of daily mseed file ( like CX.PB06..HHZ.D.2020.366)
            
            return:
                    - sliced_stream: obspy stream data
        '''
        start=obspy.UTCDateTime(starttime)
        sliced_stream = self.read_data (daily_data).slice (start,start+dt)
        return sliced_stream
    
    def apply_filter (self, stream):

        '''
        Filter the data of all traces in the Stream. This can just support "bandpass" filter.
        Parameters:
                    - stream: obspy stream data
                    - freqmin: minimum frequency
                    - freqmax: maximum frequency
        Return:
                    - filtred stream
        '''
        #sliced_stream = stream.filter('bandpass', freqmin= 1, freqmax=20)
        print(stream)
        stream[0].filter('bandpass', freqmin= 1, freqmax=20)
        stream[1].filter('bandpass', freqmin= 1, freqmax=20)
        stream[2].filter('bandpass', freqmin= 1, freqmax=20)
        
        return stream


    def plotting (self):

        '''
        This function use PhaseNet result and Catalog and creates Plot
        '''

        '''
        # Read the station names from text file and extract file name
        stations = self.get_stations ()

        # Sort stations based on latitude
        stations = self.sort_stations_latitude(stations)

        file_name = self.extract_data_name()

        # sort file name based on the lat. or long.
        file_name  = self.sort_file_name (file_name, stations)
        '''







        
        # Read the station names from text file and extract file name
        stations = self.get_stations ()

        # Sort stations based on latitude
        stations = self.sort_stations_latitude(stations)

        # Extract file name in mseed folder
        file_name = os.listdir(os.path.join(self.working_traj, "mseed"))

        # sort file name based on the lat. or long.
        file_name  = self.sort_file_name (file_name, stations)

        stream_traj =[os.path.join(os.path.join(self.working_traj, "mseed"), t) for t in file_name]

        # Read and slice data from Obspy
        stream = [self.data_slicing (self.starttime, self.dt, t) for t in stream_traj]

        # Appy filter
        stream = [self.apply_filter (k) for k in stream]

        # Read pickle data (PhaseNet result picks)
        with open(os.path.join(self.export_fig_path, "result_PhaseNet.pkl"),'rb') as fp:
            PhaseNet_result = pickle.load(fp)

        # Read the events 
        with open(os.path.join(self.export_fig_path, "events.pkl"),'rb') as fp:
            events = pickle.load(fp)
        events

        # creat a new column named network_station
        events['network_station']=events['network_code'].astype(str)+'.'+events['station_code']
        # Remove streams which are empty
        #PhaseNet_result= PhaseNet_result[PhaseNet_result['stream'].map(lambda d: len(d)) > 0]

        fig, ax = plt.subplots(PhaseNet_result.shape[0]*3,1,figsize=(40,90),constrained_layout = True)

        for i in range (0,PhaseNet_result.shape[0]):
            #stream = obspy.read(stream_traj[i])

            #stream = self.data_slicing (self.starttime, self.dt, stream)
            #stream = self.apply_filter (stream)
            #print(stream)
            st = stream[i]

            # make sure the events between start time and end time
            print(st[0].stats.endtime)
            df_sub = events[(events['picks_time']> st[0].stats.starttime) & (events['picks_time']< st[0].stats.endtime)]           
            
            # make sure the PhaseNet picks time are between start time and end time
            df_phasenet_p = PhaseNet_result.P_waves[i][(PhaseNet_result.P_waves[i]['timestamp']> st[0].stats.starttime) & (PhaseNet_result.P_waves[i]['timestamp']< st[0].stats.endtime)]
            df_phasenet_s = PhaseNet_result.S_waves[i][(PhaseNet_result.S_waves[i]['timestamp']> st[0].stats.starttime) & (PhaseNet_result.S_waves[i]['timestamp']< st[0].stats.endtime)]


            # filter station and P picks
            df_sub_p = df_sub[(df_sub['network_station']==PhaseNet_result.index[i][0:7]) & (df_sub['phase_hint']=="P")]

            # filter station and S picks
            df_sub_s = df_sub[(df_sub['network_station']==PhaseNet_result.index[i][0:7]) & (df_sub['phase_hint']=="S")]

            ax[3*i].set_title(fontsize=25,label="Station: {}".format(PhaseNet_result.index[i]), fontdict=None, loc='center')
            ax[3*i].plot(st[0].times('matplotlib'), st[0].data, 
                        markersize=1, label = 'E Stream', color = 'k')
            ax[3*i+1].plot(st[1].times('matplotlib'), st[1].data,
                        markersize=1, label = 'N Stream', color = 'k')
            ax[3*i+2].plot(st[2].times('matplotlib'), st[2].data,
                        markersize=1, label = 'Z Stream', color = 'k')

            
            plt.setp(ax[3*i].get_xticklabels(), visible=False)
            plt.setp(ax[3*i+1].get_xticklabels(), visible=False)

            
            # Draw P Picks imported from catalog

            ax[3*i].vlines([obspy.UTCDateTime(t).matplotlib_date for t in df_sub_p['picks_time'].tolist()], 
                ymin = (-st[0].max()),
                ymax = (st[0].max()),
                color='green', linestyle='dashdot', label = 'P pickes from catalog', linewidth=7.0, alpha=0.8)
            ax[3*i].xaxis_date()

            ax[3*i+1].vlines([obspy.UTCDateTime(t).matplotlib_date for t in df_sub_p['picks_time'].tolist()], 
                ymin = (-st[1].max()),
                ymax = (st[1].max()),
                color='green', linestyle='dashdot', label = 'P pickes from catalog', linewidth=7.0, alpha=0.8)
            ax[3*i+1].xaxis_date()

            
            ax[3*i+2].vlines([obspy.UTCDateTime(t).matplotlib_date for t in df_sub_p['picks_time'].tolist()], 
                ymin = (-st[2].max()),
                ymax = (st[2].max()),
                color='green', linestyle='dashdot', label = 'P pickes from catalog', linewidth=7.0, alpha=0.8)
            ax[3*i+2].xaxis_date()  

            # Draw S Picks imported from catalog           
            ax[3*i].vlines([obspy.UTCDateTime(t).matplotlib_date for t in df_sub_s['picks_time'].tolist()], 
                ymin = (-st[0].max()),
                ymax = (st[0].max()),
                color='khaki', linestyle='dashdot', label = 'S picks from catalog', linewidth=7.0, alpha=0.8)
            ax[3*i].xaxis_date()

            ax[3*i+1].vlines([obspy.UTCDateTime(t).matplotlib_date for t in df_sub_s['picks_time'].tolist()], 
                ymin = (-st[1].max()),
                ymax = (st[1].max()),
                color='khaki', linestyle='dashdot', label = 'S picks from catalog', linewidth=7.0, alpha=0.8)
            ax[3*i+1].xaxis_date()

            
            ax[3*i+2].vlines([obspy.UTCDateTime(t).matplotlib_date for t in df_sub_s['picks_time'].tolist()], 
                ymin = (-st[2].max()),
                ymax = (st[2].max()),
                color='khaki', linestyle='dashdot', label = 'S picks from catalog', linewidth=7.0, alpha=0.8)
            ax[3*i+2].xaxis_date() 
            
            # Draw P waves imported from PhaseNet

            ax[3*i].vlines([obspy.UTCDateTime(t).matplotlib_date for t in df_phasenet_p['timestamp']], 
                ymin = (-st[0].max()*np.array (df_phasenet_p['prob'])).tolist(),
                ymax = (st[0].max()*np.array (df_phasenet_p['prob'])).tolist(),
                color='b', linestyle='solid', label = 'P picks by PhaseNet', alpha=0.6)
            ax[3*i].xaxis_date()

            ax[3*i+1].vlines([obspy.UTCDateTime(t).matplotlib_date for t in df_phasenet_p['timestamp']], 
                ymin = (-st[1].max()*np.array (df_phasenet_p['prob'])).tolist(),
                ymax = ( st[1].max()*np.array (df_phasenet_p['prob'])).tolist(),
                color='b', linestyle='solid', label = 'P picks by PhaseNet', alpha=0.6)
            ax[3*i+1].xaxis_date()

            ax[3*i+2].vlines([obspy.UTCDateTime(t).matplotlib_date for t in df_phasenet_p['timestamp']], 
                ymin = (-st[2].max()*np.array (df_phasenet_p['prob'])).tolist(),
                ymax = ( st[2].max()*np.array (df_phasenet_p['prob'])).tolist(),
                color='b', linestyle='solid', label = 'P picks by PhaseNet', alpha=0.6)
            ax[3*i+2].xaxis_date()


            
            # Draw S waves imported from PhaseNet
            ax[3*i].vlines([obspy.UTCDateTime(t).matplotlib_date for t in df_phasenet_s['timestamp']], 
                ymin = (-st[0].max()*np.array (df_phasenet_s['prob'])).tolist(),
                ymax = ( st[0].max()*np.array (df_phasenet_s['prob'])).tolist(),
                color='r', linestyle='solid', label = 'S picks by PhaseNet', alpha=0.6)
            ax[3*i].xaxis_date()

            ax[3*i+1].vlines([obspy.UTCDateTime(t).matplotlib_date for t in df_phasenet_s['timestamp']], 
                ymin = (-st[1].max()*np.array (df_phasenet_s['prob'])).tolist(),
                ymax = ( st[1].max()*np.array (df_phasenet_s['prob'])).tolist(),
                color='r', linestyle='solid', label = 'S picks by PhaseNet', alpha=0.6)
            ax[3*i+1].xaxis_date()

            ax[3*i+2].vlines([obspy.UTCDateTime(t).matplotlib_date for t in df_phasenet_s['timestamp']], 
                ymin = (-st[2].max()*np.array (df_phasenet_s['prob'])).tolist(),
                ymax = ( st[2].max()*np.array (df_phasenet_s['prob'])).tolist(),
                color='r', linestyle='solid', label = 'S picks by PhaseNet', alpha=0.6)
            ax[3*i+2].xaxis_date()
            

            ax[3*i].legend(loc='lower right')
            ax[3*i+1].legend(loc='lower right')
            ax[3*i+2].legend(loc='lower right')
        file_name = '{0}{1}.{extention}'.format('PhaseNet_result_',self.starttime, extention='png')
        fig.savefig(os.path.join(self.export_fig_path, file_name), facecolor = 'w')

    def events_data_frame(self):

        '''
        This function convert catalog.xml to a data frame.
        The "events.pkl" exported to export_fig_path
        '''
        fname_out = os.path.join(self.working_traj, self.fname_cat)
        json_name = fname_out.replace(self.fname_cat, 'catalog.json')

        catalog = obspy.read_events(fname_out)
        catalog.write(json_name, format="JSON") 
        f = open(json_name)
        picks_json = json.load(f)

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

        file_name = 'events.pkl'
        events.to_pickle(os.path.join(self.export_fig_path , file_name))


if __name__ == "__main__":

    phasenet_traj = '/home/javak/phasenet_chile-subduction-zone'
    working_traj = '/home/javak/Sample_data_chile'
    station_name_list = 'CXstatlist.txt'
    starttime = obspy.UTCDateTime("2012-01-01T17:00:01.818393Z")
    dt = 3600
    fname_cat = "IPOC_picks_2012_01.xml"
    fname_inv = 'stations.xml'
    export_fig_path ='/home/javak/Sample_data_chile/Comparing PhaseNet and Catalog'

    run_PhaseNet = False
    events_DF = False
    obj = Waves_Picker (phasenet_traj, working_traj, export_fig_path, run_PhaseNet, dt, 
            starttime,fname_cat,fname_inv,events_DF, station_name_list)
    v = obj()
    print(v)

    
    