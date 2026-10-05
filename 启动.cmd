@echo off
chcp 65001 >nul
powershell -NoProfile -ExecutionPolicy Bypass -File "%~dp0_launch.ps1"
echo.
echo ==================================================
echo  如果要停止项目，双击本目录下的 停止.cmd
echo ==================================================
pause
