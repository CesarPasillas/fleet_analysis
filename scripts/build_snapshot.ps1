$ErrorActionPreference = 'Stop'

$rootDir = Split-Path -Parent $PSScriptRoot
$pythonExe = Join-Path $rootDir '.venv\Scripts\python.exe'

if (-not (Test-Path $pythonExe)) {
    Write-Host "No se encontró $pythonExe"
    Write-Host "Crea el entorno con: py -3 -m venv .venv"
    exit 1
}

$scriptPath = Join-Path $rootDir 'scripts\build_snapshot.py'
& $pythonExe $scriptPath @args
