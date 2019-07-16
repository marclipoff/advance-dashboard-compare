#!/usr/bin/env bash

cd ..

$(aws ecr get-login --no-include-email --region us-east-1)
docker build -t advance-dashboard-comparison .
docker tag advance-dashboard-comparison:latest 836434807709.dkr.ecr.us-east-1.amazonaws.com/advance-dashboard-comparison
docker push 836434807709.dkr.ecr.us-east-1.amazonaws.com/advance-dashboard-comparison:latest


