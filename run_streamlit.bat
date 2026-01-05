@echo off
setlocal
REM Run Streamlit app using repo virtualenv if present; otherwise try system python
set REPO_DIR=%~dp0
set VENV_PY=%REPO_DIR%\.venv\Scripts\python.exe
if exist "%VENV_PY%" (
  set "PYTHON=%VENV_PY%"
) else (
  set "PYTHON=python"
)
pushd "%REPO_DIR%"
echo Using Python: %PYTHON%
echo Starting Streamlit (this will open your browser)...
"%PYTHON%" -m streamlit run "%REPO_DIR%app\duckdb_app.py" --server.port 8501 --server.headless false
popd
endlocal
