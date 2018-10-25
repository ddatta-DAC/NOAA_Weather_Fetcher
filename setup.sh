#!/bin/bash
pwd;
python3 -m venv venv;
source venv/bin/activate;
python3 -m pip install --update pip;
python3 -m pip install pygrib;
python3 -m pip install requests-ftp;
python3 -m  pip install python-dateutil
mkdir Data;
