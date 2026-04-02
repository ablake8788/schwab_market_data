# Build EXE
.\.venv\Scripts\python.exe -m PyInstaller --clean --onefile --name schwab_market_data schwab_market_data.py

# Ensure dist folder exists
if (!(Test-Path ".\dist")) {
    New-Item -ItemType Directory -Path ".\dist"
}

# Copy config file
Copy-Item ".\schwab_market_data.ini" ".\dist\schwab_market_data.ini" -Force

Write-Host "Build complete. EXE and INI are in .\dist"