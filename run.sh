#!/bin/bash

# Run 3 instaneces of the docker compose file with different $NODE_NUMBER values

docker compose -f docker/docker-compose.yml up -d
