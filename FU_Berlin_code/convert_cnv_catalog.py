#! /home/javak/miniconda3/envs/phasenet/bin/python
import sys
import os

from obspy import Catalog
from obspy.core.event.event import Event
from obspy.core.event.origin import Origin
from obspy.core.event.origin import Pick
from obspy.core.event.magnitude import Magnitude
from obspy.core.event.base import WaveformStreamID
from obspy.core.event.base import QuantityError
from obspy import UTCDateTime
from obspy import read_events
from obspy import read_inventory

def main(fname_cat, fname_inv):

    # This function convert catalog which is manually created to a catalog which is proper for obspy.

    '''
    try:
        fname_cat, fname_inv = get_args(*sys.argv)
    except:
        print('Usage: %s catalog_file, inventory_xml')
    '''
    inv = read_inventory(fname_inv)
    events = []
    event_lines = []
    with open(fname_cat, 'r') as f:
        for line in f.readlines():
            if line == '\n':
                events.append(decrypt_event(event_lines, inv))
                event_lines = []
                continue
            event_lines.append(line)
    catalog = Catalog(events=events)
    fname_out = fname_cat.replace('cnv', 'xml')
    catalog.write(fname_out, format='QUAKEML')
    print('Wrote catalog to %s. (%i events)' % (fname_out, len(events)))

    # write catalogs per year
    
    year = events[0].origins[0].time.year
    year_events = []
    for event in events:
        if event.origins[0].time.year == year:
            year_events += [event]
        else:
            catalog = Catalog(events=year_events)
            fname_out = '%i.xml' % year
            catalog.write(fname_out, format='QUAKEML')
            year_events = [event]
            year = event.origins[0].time.year
    catalog = Catalog(events=year_events)
    fname_out = '%i.xml' % year
    catalog.write(fname_out, format='QUAKEML')

def decrypt_event(lines, inv):
    """
    Convert a single event from VELEST cnv format
    to ObsPy Event.
    """

    event = Event()
    o, m = decrypt_origin_magnitude(lines[0])
    picks = decrypt_picks(lines[1:], o.time, inv)
    event.origins = [o]
    if m is not None:
        event.magnitudes = [m]
    if picks:
        event.picks = picks
    return event

def decrypt_origin_magnitude(line):
    """
    Return an ObsPy Event Origin and Magnitude.
    """

    # convert origin time
    datetime = list(line[:17])
    fmt = '%y%m%d %H%M %S.%f'
    for i in [0, 7, 9, 12]:
        if datetime[i] == ' ': datetime[i] = '0'
    datetime = ''.join(datetime)
    datetime = UTCDateTime.strptime(datetime, fmt)

    # convert hypocenter
    lat = float(line[18:25])
    if line[25] == 'S': lat *= -1
    lon = float(line[27:35])
    if line[35] == 'W': lon *= -1
    depth  = float(line[36:44])

    mag = line[47:51]
    if 'nan' not in mag:
        m = Magnitude(mag=float(mag))
    else:
        m = None

    o = Origin(time=datetime,
               longitude=lon,
               latitude=lat,
               depth=depth*1e3)
    return o, m


def decrypt_picks(lines, o_time, inv):
    """
    Return a list of ObsPy Pick instances.
    """

    picks = []
    all_picks = ''.join([line[:-1] for line in lines])
    for pick_str in [all_picks[i:i+12] for i in range(0, len(all_picks), 12)]:
        sta = list(pick_str[:4])
        if sta[3] == '_': sta[3] = '*'
        sta = ''.join(sta)
        station_meta = inv.select(station=sta, time=o_time)
        if not station_meta:
            print('Station %s not found in metadata.' % sta)
            print('Skipping pick')
            continue
        if len(station_meta) > 1 or len(station_meta[0]) > 1:
            print('Station name %s too ambiguous.' % sta)
            print('Skipping pick.')
            continue
        netcode, stacode = station_meta[0].code, station_meta[0][0].code
        sta_id = WaveformStreamID(network_code=netcode, station_code=stacode)
        phase = pick_str[4].upper()
        quality = int(pick_str[5])
        e = 0.01 + quality/100
        if phase == 'S': e *= 3
        error = QuantityError(uncertainty=e)
        time = o_time + float(pick_str[6:12])
        p = Pick(time=time, phase_hint=phase, waveform_id=sta_id, time_errors=error)
        picks.append(p)
    return picks

def get_args(name, catalog_file, inventory_file):
    return catalog_file, inventory_file

if __name__ == '__main__':

    # manual pick file
    # this file should be changed from text to cnv
    fname_cat = '/home/javak/Sample_data_chile/Convert_catalog_obspy/IPOC_2007_2020_picks.cnv'

    # stations
    fname_inv = '/home/javak/Sample_data_chile/Convert_catalog_obspy/stations.xml'

    main(fname_cat,fname_inv)
