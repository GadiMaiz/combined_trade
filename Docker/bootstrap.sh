#!/bin/bash
#
# docker bootstarp script

source venv/bin/activate
echo
echo "Starting Smat-Trader..."

echo
echo "log level was set to: ${LOG_LEVEL}"

echo
echo "server is listening on port: ${SERVER_PORT}"
echo

python ./bitmain_trade_service.py -d -e ${DEFAULT_EXCHANGES} -l ${LOG_LEVEL} -p ${SERVER_PORT}