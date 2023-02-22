# tool-eda-metric-reporter

## Overview
This repository contains a tool for scraping EDA usage metric data and generating tabular reports. The reports scrape data from the following sources:
* EDA user service
* Prometheus

## Testing Locally
To test, you need to install the usagemetrics module and run the `run.py` script. You also need access to a working VEuPathDB EDA site URL and prometheus instance.

1. Install usagemetrics: `pip install .`
2. Run the script: `./bin/run.py <ENV> <EDA_URL> <PROMETHEUS_URL>`

## Jenkins Configuration
Our repo contains a Jenkinsfile to enable generating our metric reports automatically on a schedule. 