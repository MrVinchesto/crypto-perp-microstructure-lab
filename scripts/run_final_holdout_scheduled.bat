@echo off

cd /d "C:\Users\Mr_Vinchesto\Documents\projects\crypto-perp-microstructure-lab"

if not exist "reports\logs" mkdir "reports\logs"

echo ================================================== >> "reports\logs\day44_scheduled_collection.txt"
echo [START] %date% %time% >> "reports\logs\day44_scheduled_collection.txt"

".venv\Scripts\python.exe" "scripts\collect_final_holdout_runs.py" >> "reports\logs\day44_scheduled_collection.txt" 2>&1

set "EXIT_CODE=%ERRORLEVEL%"

echo [END] %date% %time% exit_code=%EXIT_CODE% >> "reports\logs\day44_scheduled_collection.txt"
echo ================================================== >> "reports\logs\day44_scheduled_collection.txt"

exit /b %EXIT_CODE%