# Run Streamlit app using repository virtualenv if available
$repo = Split-Path -Parent $MyInvocation.MyCommand.Definition
$venvPy = Join-Path $repo ".venv\Scripts\python.exe"
if (Test-Path $venvPy) {
    $python = $venvPy
}
else {
    $python = "python"
}
Write-Host "Using Python: $python"
Write-Host "Starting Streamlit (this will open your browser)..."
& $python -m streamlit run (Join-Path $repo "app\duckdb_app.py") --server.port 8501 --server.headless false
