# Crontab Setup for Bluesky AI Analysis

This directory contains crontab entries for automating tasks like embedding export and index consolidation.

## 🔧 Setup Instructions

1. Customize variables at the top of the `export_embeddings.cron` file:
   - `WORKING_DIR`
   - `DATABASE_PATH`
   - `EXPORT_DIR`
   - `VIRTUAL_ENV`
   - `LOG_DIR`

2. (Optional) Create a dedicated user account for this project.

3. Set up your crontab:

```bash
crontab crontabs/exports-crontab
```

4. Make sure the log directory exists.
