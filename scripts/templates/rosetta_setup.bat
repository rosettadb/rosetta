@echo off
REM Define the Rosetta version
set ROSETTA_VERSION=__ROSETTA_VERSION__
set "ARCH=win_x64"
set ZIP_FILE=rosetta-%ROSETTA_VERSION%-win_x64.zip
set EXTRACTION_DIR=rosetta-%ROSETTA_VERSION%-win_x64

set URL=https://github.com/AdaptiveScale/rosetta/releases/download/v%ROSETTA_VERSION%/%ZIP_FILE%

REM Download the ZIP file using curl (replace with wget if installed)
echo Downloading %ZIP_FILE%...
curl -L -o "%ZIP_FILE%" "%URL%"
if not %errorlevel%==0 (
    echo Download failed. Exiting.
    exit /b 1
)

REM Extract the ZIP file
echo Extracting %ZIP_FILE%...
powershell -Command "Expand-Archive -Path '%ZIP_FILE%' -DestinationPath '%EXTRACTION_DIR%'"
if not %errorlevel%==0 (
    echo Extraction failed. Exiting.
    exit /b 1
)

REM Cleanup: Remove the ZIP file
echo Cleaning up...
del "%ZIP_FILE%"

REM Add the directory to PATH
set "EXTRACTION_DIR=%CD%\%EXTRACTION_DIR%\%EXTRACTION_DIR%\bin"
if exist "%EXTRACTION_DIR%" (    
    echo Adding %EXTRACTION_DIR% to PATH...
    set "PATH=%NEW_VALUE%;%PATH%"
    setx PATH "%EXTRACTION_DIR%;%PATH%"
) else (
    echo Directory %EXTRACTION_DIR% does not exist. Exiting.
    exit /b 1
)

REM Prompt for project name with a default value
set /p PROJECT_NAME="Enter the project name (default: my_rosetta_project): "
if "%PROJECT_NAME%"=="" set PROJECT_NAME=my_rosetta_project

REM Run the Rosetta init command
echo Initializing Rosetta project: %PROJECT_NAME%...
rosetta init "%PROJECT_NAME%"
if not %errorlevel%==0 (
    echo Failed to initialize Rosetta project. Please check for errors.
    exit /b 1
)

echo Rosetta project '%PROJECT_NAME%' initialized successfully.
echo Next steps:
echo 1. Navigate to the project directory: cd %PROJECT_NAME%
echo 2. Configure your database connections in the 'main.conf' file inside the project directory.
echo 3. Start using Rosetta commands.