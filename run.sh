#!/bin/bash
apt-get update
apt-get install -y libgl1 libglib2.0-0
# Start your app (replace with your actual entry point)
python -m streamlit run code_creation.py --server.port 8000 --server.address 0.0.0.0