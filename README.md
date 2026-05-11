# CloudFS — Cloud Computing Mini Project

This project was built as part of my Cloud Computing course mini project.
The goal was to implement a basic cloud file transfer system using open-source
technologies, simulating HDFS (Hadoop Distributed File System) architecture
with encrypted file storage over a LAN network.

## What I Built
A working cloud storage system where:
- Files are split into 128 KB blocks (like real HDFS)
- Every block is AES-256-GCM encrypted before storage
- Blocks are replicated 3x across DataNode folders
- Any device on the same WiFi can upload/download via browser
- This simulates Software as a Service (SaaS) over LAN

## Assignment Requirements Covered
✅ Setup own cloud using open-source technologies
✅ Implemented cloud controller using Python Flask
✅ Simulated HDFS — split files into segments/blocks
✅ Upload files to cloud in encrypted form (AES-256-GCM)
✅ Download files from cloud with decryption
✅ SaaS deployment over existing LAN

## Tech Stack
- Python 3.10 / Flask 2.3
- PyCryptodome (AES-256-GCM encryption)
- HDFS simulation using local DataNode folders
- Vanilla HTML / CSS / JavaScript frontend
- REST API for all cloud operations

## How to Run
cd server
pip install -r requirements.txt
python app.py
Open http://localhost:5000 in any browser on your LAN

## College
Third Year Computer Engineering — Cloud Computing Lab