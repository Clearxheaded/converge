# Converge local dev environment setup
# Source this script to configure Go and MinGW for this project:
#   . .\env.ps1

$ProjectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path

$env:GOROOT  = Join-Path $ProjectRoot ".local\go"
$env:GOPATH  = Join-Path $ProjectRoot ".local\gopath"
$env:GOBIN   = Join-Path $env:GOPATH "bin"
$env:GOCACHE = Join-Path $ProjectRoot ".local\cache"
$env:CGO_ENABLED = "1"

$goBin = Join-Path $env:GOROOT "bin"
$mingwBin = Join-Path $ProjectRoot ".local\mingw\bin"

# Prepend project-local toolchain directories to PATH
if ($env:PATH -notlike "*$goBin*") {
    $env:PATH = "$goBin;$env:GOBIN;$env:PATH"
}
if ((Test-Path $mingwBin) -and ($env:PATH -notlike "*$mingwBin*")) {
    $env:PATH = "$mingwBin;$env:PATH"
}

# Verify
$goVersion = & (Join-Path $goBin "go.exe") version 2>&1
$gccVersion = if (Test-Path (Join-Path $mingwBin "gcc.exe")) {
    & (Join-Path $mingwBin "gcc.exe") --version | Select-Object -First 1
} else {
    "gcc not found"
}

Write-Host "Converge dev environment loaded." -ForegroundColor Green
Write-Host "  GOROOT: $env:GOROOT"
Write-Host "  GOPATH: $env:GOPATH"
Write-Host "  $goVersion"
Write-Host "  $gccVersion"
