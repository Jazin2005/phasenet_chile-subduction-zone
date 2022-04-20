from typing import Tuple
import pandas as pd
import numpy as np
import os
import pickle
import obspy
from subprocess import call
import json
import datetime
from scipy.spatial import distance_matrix
import matplotlib.pyplot as plt

from PhaseNet_Analysis import PhaseNet_Analysis

from initial_param import *

class PhaseNet_evaluation (object):

    def __init__ (self):

        self.obj =PhaseNet_Analysis (phasenet_direc,chile_GFZ_online_direc,export_DF_path, 
                export_mseed_path, working_direc, picks_name, 
                start_year_analysis, start_day_analysis,
                end_year_analysis, end_day_analysis, analysis, time_lag_threshold, station_name_list)

    def plot_roc (self):

        all_TPR = np.zeros((1, 1))
        all_FPR = np.zeros((1, 1))
        time_lag_threshold  = self.obj.time_lag_threshold 

        while time_lag_threshold < 50000:

            obj =PhaseNet_Analysis (phasenet_direc,chile_GFZ_online_direc,export_DF_path, 
                            export_mseed_path, working_direc, picks_name, 
                            start_year_analysis, start_day_analysis,
                            end_year_analysis, end_day_analysis, analysis, time_lag_threshold, station_name_list)

            precision, recall, f1_score, TPR, FPR = obj.proximity_matrix('S')
            time_lag_threshold = time_lag_threshold + 100

            all_TPR = np.vstack((all_TPR, TPR))

            all_FPR = np.vstack((all_FPR, FPR))

        fig, ax = plt.subplots(figsize=(10,10),constrained_layout = True)

        x = [0.0, 1.0]
        ax.plot(x, x, linestyle='dashed', color='red', linewidth=2, label='random')

        ax.plot(all_FPR[1:], all_TPR[1:],  color='blue', linewidth=2, label='raw_data')

        file_name = '{0}.{extention}'.format('ROC_Curve ',extention='png')
        fig.savefig(os.path.join(self.obj.export_DF_path, file_name), facecolor = 'w')


if __name__ == "__main__":

    obj = PhaseNet_evaluation()
    obj.plot_roc()









