@echo off
REM Syntax-check every Python file, then run the pytest suite (Windows).
setlocal
cd /d "%~dp0\.."
set PY=python
if exist ".venv\Scripts\python.exe" set PY=.venv\Scripts\python
echo == Compiling sources ==
%PY% -m compileall -q app.py src scripts tests || exit /b 1
echo == Running tests ==
%PY% -m pytest %*
