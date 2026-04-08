@echo off
setlocal EnableExtensions DisableDelayedExpansion

REM Graph viewer launcher for Windows (equivalent to start_graph_viewer.sh)
cd /d "%~dp0"

set "DATA_DIR=%~1"
if "%DATA_DIR%"=="" set "DATA_DIR=json_output_v4"

set "OCR_DIR=%~2"
set "PORT=10086"

if not exist "%DATA_DIR%\global_entity_index.json" (
  echo Error: %DATA_DIR%\global_entity_index.json was not found.
  echo Run pipeline first to generate data: python pipeline.py
  exit /b 1
)

set "FOUND_PID="
for /f "tokens=5" %%P in ('netstat -ano ^| findstr /R /C:":%PORT%[ ]" ^| findstr /I "LISTENING"') do (
  set "FOUND_PID=%%P"
  call :kill_pid %%P
)

if defined FOUND_PID (
  set "PORT_STILL_BUSY="
  for /f "tokens=5" %%P in ('netstat -ano ^| findstr /R /C:":%PORT%[ ]" ^| findstr /I "LISTENING"') do (
    set "PORT_STILL_BUSY=1"
  )
  if defined PORT_STILL_BUSY (
    echo Port %PORT% is still in use. Trying force kill...
    for /f "tokens=5" %%P in ('netstat -ano ^| findstr /R /C:":%PORT%[ ]" ^| findstr /I "LISTENING"') do (
      taskkill /F /PID %%P >nul 2>&1
    )
    timeout /t 1 /nobreak >nul
  )
  echo Port %PORT% has been released.
)

echo Starting graph viewer, data directory: %DATA_DIR%
echo Keep this terminal open, then visit: http://localhost:%PORT%

pushd "graph_viewer" >nul || (
  echo Error: graph_viewer directory was not found.
  exit /b 1
)
if not "%OCR_DIR%"=="" (
  echo OCR directory: %OCR_DIR%
  go run . --data "..\%DATA_DIR%" --ocr "..\%OCR_DIR%"
) else (
  go run . --data "..\%DATA_DIR%"
)
set "EXIT_CODE=%ERRORLEVEL%"
popd >nul

exit /b %EXIT_CODE%

:kill_pid
set "PID=%~1"
if "%PID%"=="0" goto :eof
if defined _SEEN_%PID% goto :eof
set "_SEEN_%PID%=1"

echo Warning: Port %PORT% is in use by PID %PID%. Trying to stop it...

set "PROCESS_NAME="
for /f "tokens=1 delims=," %%A in ('tasklist /FI "PID eq %PID%" /FO CSV /NH') do (
  set "PROCESS_NAME=%%~A"
)
if defined PROCESS_NAME (
  echo   Occupying process: %PROCESS_NAME% (PID: %PID%)
)

taskkill /PID %PID% >nul 2>&1

set /a WAIT_COUNT=0
:wait_loop
tasklist /FI "PID eq %PID%" | findstr /I /C:"%PID%" >nul
if errorlevel 1 (
  echo Process %PID% has stopped.
  goto :eof
)

if %WAIT_COUNT% GEQ 5 (
  echo Process did not exit in time, forcing termination...
  taskkill /F /PID %PID% >nul 2>&1
  timeout /t 1 /nobreak >nul
  goto :eof
)

set /a WAIT_COUNT+=1
timeout /t 1 /nobreak >nul
goto :wait_loop
