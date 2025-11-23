@echo off
echo Cleaning previous build...
if exist target (
    timeout /t 2 /nobreak > nul
    rmdir /s /q target 2>nul
)

echo Building project...
call mvn clean package -DskipTests

if %ERRORLEVEL% EQU 0 (
    echo.
    echo Build successful!
    echo JAR file location: target\media-upload-service-1.0.0.jar
) else (
    echo.
    echo Build failed. Try running: mvn clean package -DskipTests
    echo If the error persists, close any applications that might be using files in the target directory.
)
