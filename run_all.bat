@echo off
echo 🚀 Starting all Kafka consumers and orchestrator...

REM Start consumers in background and store their PIDs in temp file
start "" /B python consumers\signups_consumer.py
start "" /B python consumers\activity_consumer.py
start "" /B python consumers\billing_consumer.py
start "" /B python consumers\support_consumer.py

REM Small delay
timeout /t 5 /nobreak >nul

REM Start orchestrator in foreground
python producers\orchestrator.py

REM Wait for user to press Ctrl+C to stop
:waitLoop
echo Press Ctrl+C to stop all processes...
choice /t 1 /d y /n >nul
goto waitLoop

:stop
echo 🧹 Stopping all processes...

REM Kill only Python scripts related to consumers and orchestrator
for /f "tokens=2 delims=," %%i in ('wmic process where "CommandLine like '%%consumers%%' or CommandLine like '%%producers\\orchestrator.py%%'" get ProcessId /format:csv ^| findstr /r /v "^Node"') do (
    echo Killing PID %%i
    taskkill /PID %%i /F >nul
)

echo All processes stopped.
exit
