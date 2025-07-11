# 📂 Services Setup

This folder contains **sample `systemd` unit files** for running the Bluesky ingestion and embedding pipeline on a Linux server (VPS).

---

## ✅ How to use

1️⃣ **Copy the service file to your system**

```bash
sudo cp bluesky-ingest.service /etc/systemd/system/

2️⃣ Edit the file

Open it with sudo nano or your editor of choice.

Update:

WorkingDirectory → your actual project folder path.

ExecStart → correct .venv path and script name.

--db-path if your database lives on a mounted volume.

User= → your real Linux user name (e.g. ubuntu or davidbrandt).

3️⃣ Reload systemd

Anytime you change a .service file:

bash
Copy
Edit
sudo systemctl daemon-reload
4️⃣ Start the service

bash
Copy
Edit
sudo systemctl start bluesky-ingest
Check status:

bash
Copy
Edit
sudo systemctl status bluesky-ingest
Follow logs live:

bash
Copy
Edit
sudo journalctl -u bluesky-ingest -f
5️⃣ Enable auto-start on boot

bash
Copy
Edit
sudo systemctl enable bluesky-ingest
6️⃣ Common commands

Command	Purpose
sudo systemctl start yourservice	Start it now
sudo systemctl stop yourservice	Stop it now
sudo systemctl restart yourservice	Restart after edits
sudo systemctl status yourservice	See status & recent logs
sudo journalctl -u yourservice -f	Follow logs in real time

⚡️ Tips
✅ Always daemon-reload after editing .service files.
✅ Keep these samples versioned in Git — they’re safe, no secrets.
✅ Adjust paths & user names per machine.

