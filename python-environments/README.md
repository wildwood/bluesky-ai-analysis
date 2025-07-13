There are two different python environments for the scripts.  Anything using
huggingface embedding generator (search and embedding generation) needs the
torch environment.

## 📦 How to set up your Python venv

```bash
# Create the venv
python3 -m venv .venv # for core env
python3.11 -m venv .venv-torch # for huggingface env

# Activate the venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Deactivate when done
deactivate

# Export new requirements
pip freeze > requirements.txt