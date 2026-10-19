@echo off
REM Launch the AirScope dashboard (Windows). Creates .venv and installs requirements on first run.
setlocal
cd /d "%~dp0\.."

where java >nul 2>nul
if errorlevel 1 (
  echo ERROR: Java was not found. Spark needs JDK 17 or 21 - install Eclipse Temurin 17 and reopen this window.
  exit /b 1
)

if not exist ".venv" (
  echo Creating virtual environment in .venv ...
  py -3 -m venv .venv || python -m venv .venv
  if errorlevel 1 ( echo ERROR: could not create the virtual environment. & exit /b 1 )
  .venv\Scripts\python -m pip install --upgrade pip setuptools wheel
  .venv\Scripts\python -m pip install -r requirements.txt
  if errorlevel 1 ( echo ERROR: dependency installation failed. & exit /b 1 )
)

echo Starting AirScope at http://localhost:8501  (Ctrl+C to stop)
.venv\Scripts\python -m streamlit run app.py
