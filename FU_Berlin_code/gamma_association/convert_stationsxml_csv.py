import obspy
import csv

stations_file = obspy.core.inventory.inventory.read_inventory (path_or_file_object='/home/javak/Sample_data_chile/stations.xml', format='STATIONXML', level='response')
'read_as.networks[1].stations[1].channels[0].response.instrument_sensitivity'
v = 10

stations_object = stations_file.networks[1].stations
header = ['station','longitude','latitude','elevation(m)','unit','component','response']
data =[]
for j in stations_object:
    stations_name = j.code
    latitude = j.latitude
    longitude = j.longitude
    elevation = j.elevation
    channel_0 = j.channels[0]
    channel_0_code = j.channels[0].code
    input_units_0 = j.channels[0].response.instrument_sensitivity.input_units
    value_0 = j.channels[0].response.instrument_sensitivity.value

    channel_1 = j.channels[1]
    channel_1_code = j.channels[1].code
    value_1 = j.channels[1].response.instrument_sensitivity.value

    channel_2 = j.channels[2]
    channel_2_code = j.channels[2].code
    value_2 = j.channels[2].response.instrument_sensitivity.value

    current_row = [stations_name,
                    latitude,
                    longitude,
                    elevation, 
                    input_units_0, 
                    channel_0_code, 
                    channel_1_code, 
                    channel_2_code,
                    value_0, 
                    value_1, 
                    value_2]
    
    data.append(current_row)

with open('/home/javak/Sample_data_chile/stations_chile.csv', 'w', encoding='UTF8', newline='') as f:
    writer = csv.writer(f)

    # write the header
    writer.writerow(header)

    # write multiple rows
    writer.writerows(data)

v= 1
