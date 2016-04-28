@echo off
start "Zookeeper" /D "C:\Users\Ian\" \zookeeper-3.4.6\bin\zkServer
timeout 10
start "Nimbus, Storm" /D "C:\Users\Ian\" storm nimbus
timeout 15
start "Supervisor, Storm" /D "C:\Users\Ian\" storm supervisor
timeout 15
start "UI, Storm" /D "C:\Users\Ian\" storm ui
start "SLM, node.js" /D "C:\Users\Ian\WorkspaceGeneral\LogoManager\" node .