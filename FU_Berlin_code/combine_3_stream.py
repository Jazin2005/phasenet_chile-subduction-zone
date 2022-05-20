import pandas as pd
import numpy as np
import obspy
import json
import os
import matplotlib.pyplot as plt
import matplotlib.dates as dates

os.chdir('/home/javak/phasenet_chile-subduction-zone-main/test_data')
PROJECT_ROOT = os.getcwd()

streamZ = obspy.read(os.path.join(PROJECT_ROOT, "mseed_array/2020-10-01T00.mseed"), sep="\t")
print (streamZ)