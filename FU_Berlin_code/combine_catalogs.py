import pandas as pd
import os
import pickle

def combine_cata (export_DF_path):

    # load DF_auxiliary_path_file.pkl
    with open(os.path.join(export_DF_path, "2007_events.pkl"),'rb') as fp:
        DF_2007 = pickle.load(fp)

    with open(os.path.join(export_DF_path, "2008_events.pkl"),'rb') as fp:
        DF_2008 = pickle.load(fp)

    with open(os.path.join(export_DF_path, "2009_events.pkl"),'rb') as fp:
        DF_2009 = pickle.load(fp)

    with open(os.path.join(export_DF_path, "2010_events.pkl"),'rb') as fp:
        DF_2010 = pickle.load(fp)

    with open(os.path.join(export_DF_path, "2011_events.pkl"),'rb') as fp:
        DF_2011 = pickle.load(fp)

    with open(os.path.join(export_DF_path, "2012_events.pkl"),'rb') as fp:
        DF_2012 = pickle.load(fp)

    with open(os.path.join(export_DF_path, "2013_events.pkl"),'rb') as fp:
        DF_2013 = pickle.load(fp)

    with open(os.path.join(export_DF_path, "2014_events.pkl"),'rb') as fp:
        DF_2014 = pickle.load(fp)

    with open(os.path.join(export_DF_path, "2015_events.pkl"),'rb') as fp:
        DF_2015 = pickle.load(fp)

    with open(os.path.join(export_DF_path, "2016_events.pkl"),'rb') as fp:
        DF_2016 = pickle.load(fp)

    with open(os.path.join(export_DF_path, "2017_events.pkl"),'rb') as fp:
        DF_2017 = pickle.load(fp)

    with open(os.path.join(export_DF_path, "2018_events.pkl"),'rb') as fp:
        DF_2018 = pickle.load(fp)

    with open(os.path.join(export_DF_path, "2019_events.pkl"),'rb') as fp:
        DF_2019 = pickle.load(fp)

    with open(os.path.join(export_DF_path, "2020_events.pkl"),'rb') as fp:
        DF_2020 = pickle.load(fp)

    frames = [DF_2007,DF_2008,DF_2009,DF_2010,DF_2011,DF_2012,DF_2013,DF_2014,DF_2015,DF_2016,
            DF_2017,DF_2018,DF_2019,DF_2020]

    result = pd.concat(frames)

    file_name = 'picks_2007_2020.pkl'
    result.to_pickle(os.path.join(export_DF_path , file_name))


if __name__ == "__main__":

    export_DF_path = '/home/javak/Sample_data_chile/Comparing PhaseNet and Catalog'

    combine_cata (export_DF_path)
