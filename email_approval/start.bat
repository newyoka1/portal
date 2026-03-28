@echo off
cd /d %~dp0
if not exist .env (
    echo .env not found. Copy .env.example to .env and fill in your credentials.
    pause
    exit /b 1
)
pip install -r requirements.txt --quiet
uvicorn main:app --host 0.0.0.0 --port %PORT% --reload
