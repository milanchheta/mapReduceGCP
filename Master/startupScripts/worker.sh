sudo apt-get -y install python3-venv
cd /home/michheta/
git clone -b worker-node https://github.com/milanchheta/mapReduceGCP.git
cd mapReduceGCP/
python3 -m venv venv
source venv/bin/activate
pip3 install -r requirements.txt

cd Worker
python3 Worker.py
