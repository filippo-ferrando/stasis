#!/bin/bash

sudo rm -rf redis-data*
docker build -t blockchain -f docker/blockchain.Dockerfile .
docker build -t watchdog -f docker/watchdog.Dockerfile .
