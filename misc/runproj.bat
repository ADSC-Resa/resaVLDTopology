@echo off
start "Main Program" /D "C:\Users\Ian\WorkspaceGeneral\resaVLDTopology\target" echo storm jar resa-vld-1.0-SNAPSHOT-jar-with-dependencies.jar topology.VLDTopFox "C:/Users/Ian/Desktop/FYP2/vld_conf.yaml"
start "Input" /D "C:\Users\Ian\WorkspaceGeneral\resaVLDTopology\target" echo storm jar resa-vld-1.0-SNAPSHOT-jar-with-dependencies.jar tool.SimpleImageSenderFox C:\Users\Ian\Desktop\FYP2\vld_conf.yaml fSource 0 10 1
start "Output" /D "C:\Users\Ian\WorkspaceGeneral\resaVLDTopology\target" echo storm jar resa-vld-1.0-SNAPSHOT-jar-with-dependencies.jar server.TomVideoStreamToFile localhost 6379 tomQ
