import pandas as pd
from operaDB import OperaDB

class OmronEye:
    def __init__(self, operaDB):
        self.opera = operaDB
        self.tables = 'driveragent_omroncamera'

    def get_DataFrameFromTrip(self, tripLists, l, offset=0):
        return self.opera.get_DataFromTrip( self.tables, tripLists, l, offset )

    def get_DataFrameFromTime(self, tstart, duration ):
        return self.opera.get_DataFromTime( self.tables, tstart, duration )
