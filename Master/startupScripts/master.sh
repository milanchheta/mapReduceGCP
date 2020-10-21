sudo apt-get -y install python3-venv

git clone -b master-node https://github.com/milanchheta/mapReduceGCP.git
cd mapReduceGCP/
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt

cd Master
python3 Master.py
