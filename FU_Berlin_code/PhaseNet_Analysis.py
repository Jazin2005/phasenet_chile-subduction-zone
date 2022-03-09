import pandas as pd
import numpy as np
import os
import pickle
import obspy
from subprocess import call
import json



class PhaseNet_Analysis (object):

    '''
    This class analysis and compare the 'P' peaks and 'S' Peaks
    with the given catalog
    '''

    def __init__(self,phasenet_direc: 'str', chile_GFZ_online_direc:'str', export_DF_path:'str',
                export_mseed_path:'str', working_direc:'str',
                start_year_analysis:'int', start_day_analysis:'int', 
                end_year_analysis:'int', end_day_analysis:'int'):

        '''
        Parameters initialization:
            - phasenet_direc: The path of PhaseNet package
            - chile_GFZ_online_direc: The directory of stored all three components mseed files.
            - export_DF_path : The path of all mseed files DataFrame.
                                This directory must contain 'DF_chile_path_file.pkl'.
                                This DataFrame is created once using "generate_DF_file_path" function.
                                This function is not used often.
                                After run "DF_path" function, another two DataFrames
                                ("DF_selected_chile_path_file.pkl" and "DF_auxiliary_path_file.pkl") 
                                will be created and stored in this directory as a feed path to run PhaseNet.
            - export_mseed_path: The folder path of writing three components mseed.
                This folder will be used by PhaseNet.
            - working_direc: the path of working directory
            - start_year_analysis: start year of analysis with 4 digit (like 2011)
            - start_day_analysis: start day of analysis related to 
                                the start_year_analysis. This variable should between 1 to 365. 

            - end_year_analysis: end year of analysis with 4 digit (like 2011)
            - end_day_analysis: end day of analysis related to 
                                the end_year_analysis. This variable should between 1 to 365.         
        '''
        os.chdir('{0}'.format(phasenet_direc))
        self.PROJECT_ROOT = os.getcwd()

        self.chile_GFZ_online_direc = chile_GFZ_online_direc
        self.export_DF_path = export_DF_path
        self.export_mseed_path = export_mseed_path
        self.working_direc = working_direc
        self.start_year_analysis = start_year_analysis
        self.start_day_analysis = start_day_analysis
        self.end_year_analysis = end_year_analysis
        self.end_day_analysis = end_day_analysis

    
    def __call__ (self):


        # creat "DF_auxiliary_path_file.pkl" and "DF_selected_chile_path_file.pkl" based on the
        # given interval.

        self.DF_path ()

        # load DF_auxiliary_path_file.pkl
        with open(os.path.join(self.export_DF_path, "DF_auxiliary_path_file.pkl"),'rb') as fp:
            DF_auxiliary_path_file = pickle.load(fp)

        # load DF_selected_chile_path_file.pkl
        with open(os.path.join(self.export_DF_path, "DF_selected_chile_path_file.pkl"),'rb') as fp:
            DF_selected_chile_path_file = pickle.load(fp)
        
        # Creates pandas DataFrame.
        data = {'P_waves':[],'S_waves':[]}
        df_picks = pd.DataFrame(data, index =[]) 


        # Iterate over mseed files which contains 3 components mseed file.
        
        #for i in range (DF_auxiliary_path_file.shape[0]):
        for i in range (4):

            # Write mseed file to mseed folder path
            mseed_name = self.three_components_mseed_maker (i,DF_auxiliary_path_file, 
                                    DF_selected_chile_path_file)
            
            # Write the name of mseed file in mseed.csv
            self.write_mseed_names(mseed_name)

            # Run PhaseNet
            self.run_phasenet()

            # Remove created mseed in mseed folder to free up memory
            self.remove_mseed (mseed_name)

            # Read the output of PhaseNet and store P picks and S picks in two data frames
            df_p_picks, df_s_picks = self.read_picks()

            # save data in data frame
            df_total = self.save_DF (df_p_picks, df_s_picks, mseed_name, df_picks)
            df_picks = df_total

        # save file in the export directory
        phasenet_file_name = 'PhaseNet_result.pkl'
        df_picks.to_pickle(os.path.join(self.export_DF_path, phasenet_file_name))

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
    
        
    
    
    
    def DF_path (self):

        '''
        This function loads "DF_chile_path_file.pkl" file (path of all mseed files
        from export_DF_path and filter the data between the given interval.
        After using this function, 'DF_selected_chile_path_file.pkl' and 
        'DF_auxiliary_path_file.pkl' will be created to feed data to PhaseNet.

        Important note: This function will not consider mseed file with less than 3- components.
        '''

        # Read pickle data (Path of all chile stream data)
        with open(os.path.join(self.export_DF_path, "DF_chile_path_file.pkl"),'rb') as fp:
            chile_path_file = pickle.load(fp)

        chile_path_file = chile_path_file[(chile_path_file['year']>= self.start_year_analysis) & 
                    (chile_path_file['year']<= self.end_year_analysis)]

        chile_path_file['convert_yeartoday']= 365*chile_path_file['year']+chile_path_file['day']
        
        # creat upper and lower limit to filter
        lower_limit = 365*self.start_year_analysis + self.start_day_analysis 
        upper_limit = 365*self.end_year_analysis   + self.end_day_analysis

        # Apply filter
        chile_path_file = chile_path_file[(chile_path_file['convert_yeartoday']>= lower_limit) & 
                (chile_path_file['convert_yeartoday']<= upper_limit)]   

        chile_path_file = chile_path_file.drop_duplicates()

        # creat new DataFrame to make sure all 3-components are existed
        df_counter = chile_path_file.groupby(['network','station', 'year', 'day']).size().reset_index(name='count')
        df_counter = df_counter[df_counter['count']==3]

        # drop the 'count' column
        df_counter = df_counter.drop(columns=['count'])

        # Save selected DataFrame based on given time interval
        chile_path_file.to_pickle(os.path.join(self.export_DF_path , 'DF_selected_chile_path_file.pkl'))

        # Save auxiliary DataFrame based on given time interval
        df_counter.to_pickle(os.path.join(self.export_DF_path , 'DF_auxiliary_path_file.pkl'))

    
    def three_components_mseed_maker (self,i:'int',DF_auxiliary_path_file, 
                                    DF_selected_chile_path_file):
        '''
        This function write a single three components mseed file in mseed folder.
        Parameters:
                    - i (int): the ith row of "DF_auxiliary_path_file" data frame
                    - DF_auxiliary_path_file (data frame): A data frame containing mseed file name 
                        with existed three componets.
                    - DF_selected_chile_path_file(data frame): A selected data frame based on the given time interval.
                    
        '''



        # Apply filter to determine the three components in "DF_selected_chile_path_file"
        df = DF_selected_chile_path_file[['network', 'station', 'year','day']]==DF_auxiliary_path_file.iloc[i]
        df_components = DF_selected_chile_path_file[(df['network']== True) & 
                        (df['station']== True) & (df['year']== True) &
                        (df['day']== True)]

        # Read three components mseed file 
        streamZ = obspy.read(df_components.path.iloc[2])
        streamN = obspy.read(df_components.path.iloc[1])
        streamE = obspy.read(df_components.path.iloc[0])
        streamN += streamE
        streamN += streamZ
        stream = streamN.sort()

        # Write the mseed file (three components) in mseed folder
        string = df_components.file_name.iloc[2]
        mseed_name = string.replace("HHE", "").replace("HHN", "").replace("HHZ", "")
        stream.write(os.path.join(self.export_mseed_path, mseed_name), sep="\t", format="MSEED")

        return mseed_name

    def write_mseed_names(self, mseed_name):
        '''
        This function write the name of mseed file to mseed.csv.
        This mseed.csv will be used by PhaseNet
        '''
        df = pd.DataFrame ([mseed_name], columns = ['fname'])

        df.to_csv((os.path.join(self.working_direc, 'mseed.csv')),index=False)
    
    def run_phasenet (self):
        '''
        Run the predefined PhaseNet model (model/190703-214543) to pick S and P waves.
        '''
        cmd = '/home/javak/miniconda3/envs/phasenet/bin/python phasenet/predict.py --model=model/190703-214543 --data_list=/home/javak/Sample_data_chile/mseed.csv --data_dir=/home/javak/Sample_data_chile/mseed --format=mseed --plot_figure'.split()
        call(cmd)
    
    def read_picks (self):

        '''
        Read the csv file of PhaseNet output and return the P picks and S picks.
        '''
        picks_csv = pd.read_csv(os.path.join(self.PROJECT_ROOT, "results/picks.csv"), sep="\t")
        picks_csv.loc[:, 'p_idx'] = picks_csv["p_idx"].apply(lambda x: x.strip("[]").split(","))
        picks_csv.loc[:, 'p_prob'] = picks_csv["p_prob"].apply(lambda x: x.strip("[]").split(","))
        picks_csv.loc[:, 's_idx'] = picks_csv["s_idx"].apply(lambda x: x.strip("[]").split(","))
        picks_csv.loc[:, 's_prob'] = picks_csv["s_prob"].apply(lambda x: x.strip("[]").split(","))

        with open(os.path.join(self.PROJECT_ROOT, "results/picks.json")) as fp:
            picks_json = json.load(fp)
        
        df = pd.DataFrame.from_dict(pd.json_normalize(picks_json), orient='columns')
        df_p_picks = df[df["type"] == 'p']
        df_s_picks = df[df["type"] == 's']

        return df_p_picks, df_s_picks
    
    def remove_mseed (self,mseed_name):

        '''
        This function removes created mseed in mseed folder to free up memory.
        '''

        file_path = os.path.join(self.export_mseed_path, mseed_name)
        os.remove(file_path)
    
    def save_DF (self, df_p_waves, df_s_waves, daily_data, df):

        data = {'P_waves':[df_p_waves],
                'S_waves':[df_s_waves]
                }

        # Creates pandas DataFrame.
        df_new = pd.DataFrame(data, index =[daily_data])
        df_total = df.append(df_new)
        return df_total



if __name__ == "__main__":

    phasenet_direc = '/home/javak/phasenet_chile-subduction-zone'
    chile_GFZ_online_direc = '/data2/chile/CHILE_GFZ_ONLINE'
    export_DF_path = '/home/javak/Sample_data_chile/Comparing PhaseNet and Catalog'
    export_mseed_path = '/home/javak/Sample_data_chile/mseed'
    working_direc = '/home/javak/Sample_data_chile'
    start_year_analysis = 2012
    start_day_analysis = 1
    end_year_analysis = 2012
    end_day_analysis = 31

    obj = PhaseNet_Analysis (phasenet_direc,chile_GFZ_online_direc,export_DF_path, 
                            export_mseed_path, working_direc,
                            start_year_analysis, start_day_analysis,
                            end_year_analysis, end_day_analysis)
    
    result = obj()