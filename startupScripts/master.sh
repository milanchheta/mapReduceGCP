sudo apt-get update
sudo apt-get -y install python3-venv
cd /home/michheta/

git clone -b master-node https://github.com/milanchheta/mapReduceGCP.git
cd mapReduceGCP/
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt
python3 Master.py
