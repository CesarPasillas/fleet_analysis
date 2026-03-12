$ErrorActionPreference = 'Stop'

$rootDir = Split-Path -Parent $PSScriptRoot
$backendHost = if ($env:BACKEND_HOST) { $env:BACKEND_HOST } else { '127.0.0.1' }
$backendPort = if ($env:BACKEND_PORT) { $env:BACKEND_PORT } else { '8000' }
$frontendHost = if ($env:FRONTEND_HOST) { $env:FRONTEND_HOST } else { '127.0.0.1' }
$frontendPort = if ($env:FRONTEND_PORT) { $env:FRONTEND_PORT } else { '5173' }

$pythonExe = Join-Path $rootDir '.venv\Scripts\python.exe'
if (-not (Test-Path $pythonExe)) {
    Write-Host "No se encontró $pythonExe"
    Write-Host "Crea el entorno con: py -3 -m venv .venv"
    exit 1
}

$backendArgs = @(
    '-m', 'uvicorn',
    'app.main:app',
    '--app-dir', (Join-Path $rootDir 'backend'),
    '--host', $backendHost,
    '--port', $backendPort,
    '--reload'
)

$backendProc = Start-Process -FilePath $pythonExe -ArgumentList $backendArgs -PassThru

try {
    npm --prefix (Join-Path $rootDir 'frontend') run dev -- --host $frontendHost --port $frontendPort
}
finally {
    if ($backendProc -and -not $backendProc.HasExited) {
        Stop-Process -Id $backendProc.Id -Force
    }
}
