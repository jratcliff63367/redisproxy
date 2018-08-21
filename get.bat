@echo off
set SOURCE=F:\nvidiagit\backend\api_server_cpp\src
set SOURCE_INCLUDE=f:\nvidiagit\backend\api_server_cpp\include

cd include
copy %SOURCE_INCLUDE%\KeyValueDatabase.h
copy %SOURCE_INCLUDE%\RedisCommandStream.h
cd ..
cd src
copy %SOURCE%\InputLine.cpp
copy %SOURCE%\KeyValueDatabase.cpp
copy %SOURCE%\KeyValueDatabaseRedis.cpp
copy %SOURCE%\RedisCommandStream.cpp
cd ..

