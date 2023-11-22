import threading
import station_pb2_grpc, station_pb2
import traceback
from cassandra import ConsistencyLevel
import cassandra
from cassandra.cluster import Cluster
import pandas as pd

class Station(station_pb2_grpc.StationServicer):
    def __init__(self):
        print("initializing station")
        self.cluster = Cluster(['p6-db-1', 'p6-db-2', 'p6-db-3'])
        self.cass = self.cluster.connect()
    
    def RecordTemps(self, request, context):
        try:
            insert_statement = self.cass.prepare("INSERT INTO weather.stations (id, date, record) VALUES (?, ?, {tmin: ?, tmax: ?})")
            insert_statement.consistency_level = ConsistencyLevel.ONE
            year = request.date[0:4]
            month = request.date[4:6]
            day = request.date[6:]
            self.cass.execute(insert_statement, (request.station, f"{year}-{month}-{day}", request.tmin, request.tmax))
            return station_pb2.RecordTempsReply(error = "")
        except cassandra.Unavailable as e1:
            return station_pb2.RecordTempsReply(error = f"need {e1.required_replicas} replicas, but only have {e1.alive_replicas}")
        except cassandra.cluster.NoHostAvailable as e2:
            for err in e.errors:
                if (err==cassandra.Unavailable):
                    return station_pb2.RecordTempsReply(error = f"need {err.required_replicas} replicas, but only have {err.alive_replicas}")
        except Exception as e:
            return station_pb2.RecordTempsReply(error = e)
  
    def StationMax(self, request, context):
        try:
            max_statement = self.cass.prepare("SELECT record.tmax from weather.stations WHERE id=?")
            max_statement.consistency_level = ConsistencyLevel.THREE
            df = pd.DataFrame(self.cass.execute(max_statement, (request.station,)))
            tmaxVal = float('-inf')
            for index, row in df.iterrows():
                if row["record_tmax"] > tmaxVal:
                    tmaxVal = row["record_tmax"]
            return station_pb2.StationMaxReply(tmax = tmaxVal, error = "")
        except cassandra.Unavailable as e1:
            return station_pb2.StationMaxReply(error = f"need {e1.required_replicas} replicas, but only have {e1.alive_replicas}")
        except cassandra.cluster.NoHostAvailable as e2:
            for err in e.errors:
                if (err==cassandra.Unavailable):
                    return station_pb2.StationMaxReply(error = f"need {err.required_replicas} replicas, but only have {err.alive_replicas}")
        except Exception as e:
            return station_pb2.StationMaxReply(error = e)

# Server Code
import grpc
from concurrent import futures
#if __name__ == "__main__":
#    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=(('grpc.so_reuseport', 0),))
#    station_pb2_grpc.add_StationServicer_to_server(Station(), server)
#    server.add_insecure_port("0.0.0.0:5440", )
#    server.start()
#    print("started")
#    server.wait_for_termination()

server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=(('grpc.so_reuseport', 0),))
station_pb2_grpc.add_StationServicer_to_server(Station(), server)
server.add_insecure_port("0.0.0.0:5440", )
server.start()
print("started")
server.wait_for_termination()
