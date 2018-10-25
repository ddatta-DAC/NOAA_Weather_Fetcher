
#!/bin/bash
# [ubuntu]      Requires installation of  packages.
#	Steps :
# Conda Python 2.7
conda install -c conda-forge pygrib
conda install -c conda-forge requests-ftp 
conda install --channel https://conda.anaconda.org/anaconda mysql-python
conda install -c conda-forge dateutils 

sudo apt-get mysql
sudo apt-get mysql-server-core-5.7
sudo apt-get mysql-server
sudo apt-get install mysql-client
sudo apt-get install mysql-common 

#       Check stataus : 
sudo service mysql status

mkdir Data

