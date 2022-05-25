import pandas as pd
from datetime import datetime, timedelta
from gamma import BayesianGaussianMixture, GaussianMixture
from gamma.utils import convert_picks_csv, association, from_seconds
import numpy as np
from sklearn.cluster import DBSCAN 
from datetime import datetime, timedelta
import os
import json
import pickle
from tqdm import tqdm



class gamma_associator (object):

    def __init__ (self, files_directory:'str', stations:'str',
                        center:'tuple', xlim_degree:'list', ylim_degree:'list',degree2km:'float',
                        starttime:'datetime', endtime:'datetime',
                        p_picks:'str', s_picks:'str'):

        '''
        Initialization:
                    - files_directory('str'): The directory of existed files for association
                    - stations (str): the name and properties of each station.
                                    this file should contains the following columns:
                                            1- station: station name
                                            2- longitude
                                            3- latitude
                                            4- elevation(m)
                                            5- unit: m/s or m/s2
                                            6- component: N,E,Z
                                            7- response

                    - center (tuple): The longitude and latitiude of center location for association. Example (-117, 35), -117 is longitude and 35 is latitude.
                    - xlim_degree (list): the longitude interval for association. example [-117, -118].
                    - ylim_degree (list): the latitiude interval for association. example [35, 36].
                    - degree2km (float): the z direction degree. example: 111.19492474777779.
                    - starttime (datetime): start time for association. example: datetime(2019, 7, 4, 17, 0).
                    - endtime (datetime): end time for association.     example: datetime(2019, 7, 5, 17, 0).
                    - p_picks ('str'): the name of p_picks pkl file. example: p_picks.pkl
                                    This files contains 8 columns:
                                            1- id
                                            2- timestamp
                                            3- prob : phasenet probability
                                            4- type : phase hint (P or S)
                                            5- amp_p_0: amplitude of first trace
                                            6- amp_p_1: amplitude of second trace
                                            7- amp_p_2: amplitude of third trace
                                            8- p_phase_amp: two norm of amplitude

                    - s_picks ('str'): the name of s_picks pkl file. example: s_picks.pkl
                                    This files contains 8 columns:
                                            1- id
                                            2- timestamp
                                            3- prob : phasenet probability
                                            4- type : phase hint (P or S)
                                            5- amp_p_0: amplitude of first trace
                                            6- amp_p_1: amplitude of second trace
                                            7- amp_p_2: amplitude of third trace
                                            8- p_phase_amp: two norm of amplitude 

        '''
        self.files_directory = files_directory
        self.stations = stations
        self.center = center
        self.xlim_degree = xlim_degree
        self.ylim_degree = ylim_degree
        self.degree2km = degree2km
        self.starttime = starttime
        self.endtime = endtime
        self.p_picks = p_picks
        self.s_picks = s_picks

        # set up config for association
        self.config = {'center': self.center, 
                'xlim_degree': self.xlim_degree, 
                'ylim_degree': self.ylim_degree, 
                'degree2km': self.degree2km, 
                'starttime': self.starttime, 
                'endtime': self.endtime}

        ## read picks
        with open(os.path.join(self.files_directory, "test_chile/p_picks.pkl"),'rb') as fp:
                 p_picks = pickle.load(fp)
        
        with open(os.path.join(self.files_directory, "test_chile/s_picks.pkl"),'rb') as fs:
                 s_picks = pickle.load(fs)


        # concatenate p_picks and s_picks
        self.picks = pd.concat([p_picks, p_picks])

        # convert string into timestamp
        self.picks["timestamp"] = pd.to_datetime(self.picks["timestamp"])
        
        ## process by hours
        self.picks["time_idx"] = self.picks["timestamp"].apply(lambda x: x.strftime("%Y-%m-%dT%H")) 

        ## read stations
        self.stations = pd.read_csv('{0}{1}{2}'.format(self.files_directory, '/test_chile/', self.stations))
        self.stations = self.stations.rename(columns={"station":"id"})
        self.stations["x(km)"] = self.stations["longitude"].apply(lambda x: (x - self.config["center"][0])*self.config["degree2km"])
        self.stations["y(km)"] = self.stations["latitude"].apply(lambda x: (x - self.config["center"][1])*self.config["degree2km"])
        self.stations["z(km)"] = self.stations["elevation(m)"].apply(lambda x: -x/1e3)


        ### setting GMMA configs
        self.config["dims"] = ['x(km)', 'y(km)', 'z(km)']
        self.config["use_dbscan"] = True
        self.config["use_amplitude"] = True
        self.config["x(km)"] = (np.array(self.config["xlim_degree"])-np.array(self.config["center"][0]))*self.config["degree2km"]
        self.config["y(km)"] = (np.array(self.config["ylim_degree"])-np.array(self.config["center"][1]))*self.config["degree2km"]
        self.config["z(km)"] = (0, 20)
        self.config["vel"] = {"p": 6.0, "s": 6.0 / 1.75}
        self.config["method"] = "BGMM"
        if self.config["method"] == "BGMM":
            self.config["oversample_factor"] = 4
        if self.config["method"] == "GMM":
            self.config["oversample_factor"] = 1

        # DBSCAN
        self.config["bfgs_bounds"] = (
            (self.config["x(km)"][0] - 1, self.config["x(km)"][1] + 1),  # x
            (self.config["y(km)"][0] - 1, self.config["y(km)"][1] + 1),  # y
            (0, self.config["z(km)"][1] + 1),  # x
            (None, None),  # t
        )
        self.config["dbscan_eps"] = min(
            6, #seconds
            np.sqrt(
                (self.stations["x(km)"].max() - self.stations["x(km)"].min()) ** 2
                + (self.stations["y(km)"].max() - self.stations["y(km)"].min()) ** 2
            )
            / (6.0 / 1.75),
        )
        self.config["dbscan_min_samples"] = min(3, len(self.stations))

        for k, v in self.config.items():
            print(f"{k}: {v}")






if __name__ == "__main__":

        files_directory = '/home/javak/phasenet_chile-subduction-zone-main/FU_Berlin_code/gamma_association'
        stations = 'stations_chile.csv'
        center = (-117.504, 35.705)
        xlim_degree = [-118.004, -117.004]
        ylim_degree = [35.205, 36.205]
        degree2km = 111.19492474777779
        starttime = datetime(2019, 7, 4, 17, 0)
        endtime = datetime(2019, 7, 5, 17, 0)
        p_picks = "PhaseNet_result_p_picks.pkl"
        s_picks = "PhaseNet_result_p_picks.pkl"

        obj = gamma_associator(files_directory ,stations,
                        center,
                        xlim_degree,
                        ylim_degree,
                        degree2km,
                        starttime,
                        endtime,
                        p_picks,
                        s_picks)
        
        obj.__init__()
























'''
data_dir = lambda x: os.path.join("test_data", x)
station_csv = data_dir("stations.csv")
pick_json = data_dir("picks.json")
catalog_csv = data_dir("catalog_gamma.csv")
picks_csv = data_dir("picks_gamma.csv")

if not os.path.exists("figures"):
    os.makedirs("figures")
figure_dir = lambda x: os.path.join("figures", x)

config = {'center': (-117.504, 35.705), 
    'xlim_degree': [-118.004, -117.004], 
    'ylim_degree': [35.205, 36.205], 
    'degree2km': 111.19492474777779, 
    'starttime': datetime(2019, 7, 4, 17, 0), 
    'endtime': datetime(2019, 7, 5, 0, 0)}

## read picks
picks = pd.read_json('/home/javak/phasenet_chile-subduction-zone-main/FU_Berlin_code/gamma_association/test_data/picks.json')
picks["time_idx"] = picks["timestamp"].apply(lambda x: x.strftime("%Y-%m-%dT%H")) ## process by hours

## read stations
stations = pd.read_csv('/home/javak/phasenet_chile-subduction-zone-main/FU_Berlin_code/gamma_association/test_data/stations.csv', delimiter="\t")
stations = stations.rename(columns={"station":"id"})
stations["x(km)"] = stations["longitude"].apply(lambda x: (x - config["center"][0])*config["degree2km"])
stations["y(km)"] = stations["latitude"].apply(lambda x: (x - config["center"][1])*config["degree2km"])
stations["z(km)"] = stations["elevation(m)"].apply(lambda x: -x/1e3)

### setting GMMA configs
config["dims"] = ['x(km)', 'y(km)', 'z(km)']
config["use_dbscan"] = True
config["use_amplitude"] = True
config["x(km)"] = (np.array(config["xlim_degree"])-np.array(config["center"][0]))*config["degree2km"]
config["y(km)"] = (np.array(config["ylim_degree"])-np.array(config["center"][1]))*config["degree2km"]
config["z(km)"] = (0, 20)
config["vel"] = {"p": 6.0, "s": 6.0 / 1.75}
config["method"] = "BGMM"
if config["method"] == "BGMM":
    config["oversample_factor"] = 4
if config["method"] == "GMM":
    config["oversample_factor"] = 1

# DBSCAN
config["bfgs_bounds"] = (
    (config["x(km)"][0] - 1, config["x(km)"][1] + 1),  # x
    (config["y(km)"][0] - 1, config["y(km)"][1] + 1),  # y
    (0, config["z(km)"][1] + 1),  # x
    (None, None),  # t
)
config["dbscan_eps"] = min(
    6, #seconds
    np.sqrt(
        (stations["x(km)"].max() - stations["x(km)"].min()) ** 2
        + (stations["y(km)"].max() - stations["y(km)"].min()) ** 2
    )
    / (6.0 / 1.75),
)
config["dbscan_min_samples"] = min(3, len(stations))

# Filtering
config["min_picks_per_eq"] = min(10, len(stations) // 2)
config["max_sigma11"] = 2.0
config["max_sigma22"] = 1.0
config["max_sigma12"] = 1.0

for k, v in config.items():
    print(f"{k}: {v}")






pbar = tqdm(sorted(list(set(picks["time_idx"]))))
event_idx0 = 0 ## current earthquake index
assignments = []
if (len(picks) > 0) and (len(picks) < 5000):
    catalogs, assignments = association(picks, stations, config, event_idx0, config["method"], pbar=pbar)
    event_idx0 += len(catalogs)
else:
    catalogs = []
    for i, hour in enumerate(pbar):
        picks_ = picks[picks["time_idx"] == hour]
        if len(picks_) == 0:
            continue
        catalog, assign = association(picks_, stations, config, event_idx0, config["method"], pbar=pbar)
        event_idx0 += len(catalog)
        catalogs.extend(catalog)
        assignments.extend(assign)

## create catalog
catalogs = pd.DataFrame(catalogs, columns=["time(s)"]+config["dims"]+["magnitude", "sigma_time", "sigma_amp", "cov_time_amp",  "event_idx", "prob_gamma"])
catalogs["time"] = catalogs["time(s)"].apply(lambda x: from_seconds(x))
catalogs["longitude"] = catalogs["x(km)"].apply(lambda x: x/config["degree2km"] + config["center"][0])
catalogs["latitude"] = catalogs["y(km)"].apply(lambda x: x/config["degree2km"] + config["center"][1])
catalogs["depth(m)"] = catalogs["z(km)"].apply(lambda x: x*1e3)
with open(catalog_csv, 'w') as fp:
    catalogs.to_csv(fp, sep="\t", index=False, 
                    float_format="%.3f",
                    date_format='%Y-%m-%dT%H:%M:%S.%f',
                    columns=["time", "magnitude", "longitude", "latitude", "depth(m)", "sigma_time", "sigma_amp", "cov_time_amp", "event_idx", "prob_gamma"])
catalogs = catalogs[['time', 'magnitude', 'longitude', 'latitude', 'depth(m)', 'sigma_time', 'sigma_amp', 'prob_gamma']]

## add assignment to picks
assignments = pd.DataFrame(assignments, columns=["pick_idx", "event_idx", "prob_gamma"])
picks = picks.join(assignments.set_index("pick_idx")).fillna(-1).astype({'event_idx': int})
with open(picks_csv, 'w') as fp:
    picks.to_csv(fp, sep="\t", index=False, 
                    date_format='%Y-%m-%dT%H:%M:%S.%f',
                    columns=["id", "timestamp", "type", "prob", "amp", "event_idx", "prob_gamma"])
'''