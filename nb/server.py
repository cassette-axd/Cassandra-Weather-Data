import threading
import station_pb2_grpc, station_pb2
import torch
import numpy as np
import pandas
import traceback

class Station(station_pb2_grpc.StationServicer):
    def __init__(self):
        print("initializing station")

    
    def RecordTemps(self):
        pass

    
    def StationMax(self):
        pass


