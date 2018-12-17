import pandas as pd
from operaDB import OperaDB

class OBD:
    def __init__(self, operaDB):
        self.opera = operaDB
        self.tables = 'driveragent_obd'

    def get_DataFrameFromTrip(self, tripLists, l, offset=0):
        return self.opera.get_DataFromTrip( self.tables, tripLists, l, offset )

    def get_DataFrameFromTime(self, tstart, tend ):
        return self.opera.get_DataFromTime( self.tables, tstart, tend )

class GPS:
    def __init__(self, operaDB):
        self.opera = operaDB
        self.tables = 'driveragent_location'

    def get_DataFrameFromTrip(self, tripLists, l, offset=0):
        return self.opera.get_DataFromTrip( self.tables, tripLists, l, offset )

    def get_DataFrameFromTime(self, tstart, tend ):
        return self.opera.get_DataFromTime( self.tables, tstart, tend )

class Sensors:
    def __init__(self, operaDB):
        self.opera = operaDB
        self.tables = 'driveragent_sensor'

    def get_ListSensorType(self):
        print('sensor name : sensor type')
        print('BMP280 temperature : 65536')
        print('BMI160 accelerometer : 1')
        print('BMM150 magnetometer : 2')
        print('Orientation : 3')
        print('BMI160 gyroscope : 4')
        print('TMD27723 Light Sensor : 5')
        print('BMP280 pressure : 6')
        print('Gravity : 9')
        print('Linear Acceleration : 10')
        print('Rotation Vector : 11')
        print('BMM150 magnetometer (uncalibrated) : 14')
        print('Game Rotation Vector : 15')
        print('BMI160 gyroscope (uncalibrated) : 16')
        print('Geomagnetic Rotation Vector : 20')
        print('BMI160 accelerometer (uncalibrated) : 35')
        print('You need more information, Please see below')
        print('https://developer.android.com/reference/android/hardware/Sensor')

    def get_DataFrameFromTrip(self, tripLists, l, offset=0):
        return self.opera.get_DataFromTrip( self.tables, tripLists, l, offset )

    def get_DataFrameFromTime(self, tstart, tend ):
        return self.opera.get_DataFromTime( self.tables, tstart, tend )

    def get_DataFrameFromTripWithSensorType(self, sensorType, tripLists, l, offset=0):
        start_datetime, stop_datetime = self.opera.get_TripTime(tripLists, l)

        #Create Query
        query = 'SELECT * FROM ' + self.tables
        datetime_conditions = ' WHERE time > ' + str(start_datetime+offset*1000) + ' AND ' + 'time < ' + str(stop_datetime)
        sensor_opt = ' sensor_type = ' + str(sensorType)
        query = query + datetime_conditions + ' AND ' + sensor_opt + ';'
        #print(query)

        column_names, df = self.opera.get_DataFrame( query )
        return column_names, df

    def get_DataFrameFromTimeWithSensorType(self, sensorType, tstart, tend ):
        #Create Query
        query = 'SELECT * FROM ' + self.tables
        datetime_conditions = ' WHERE time > ' + str(tstart) + ' AND ' + 'time < ' + str(tend)
        sensor_opt = ' sensor_type = ' + str(sensorType)
        query = query + datetime_conditions + ' AND ' + sensor_opt + ';'
        #print(query)

        column_names, df = self.opera.get_DataFrame( query )
        return column_names, df

    def get_values(self, df ):
        vals = []
        for s in df:
            l = [float(x.strip()) for x in s.split(',')]
            vals.append(l)

        return len(vals[0]), vals

class UploadStatus:
    def __init__(self, operaDB):
        self.opera = operaDB
        self.tables = 'driveragent_uploadstatus'

    def get_DataFrameFromTrip(self, tripLists, l, offset=0):
        return self.opera.get_DataFromTrip( self.tables, tripLists, l, offset )

    def get_DataFrameFromTime(self, tstart, tend ):
        return self.opera.get_DataFromTime( self.tables, tstart, tend )
