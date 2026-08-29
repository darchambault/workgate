# Builds workgate.exe and installs it to %LOCALAPPDATA%\Programs\workgate,
# registering that directory on the user PATH if needed.
#
#   .\install.ps1              # build, test, install
#   .\install.ps1 -SkipTests   # build, install
#
# Idempotent: safe to re-run after every source change.

[CmdletBinding()]
param(
    [switch]$SkipTests
)

$ErrorActionPreference = "Stop"
$repo = $PSScriptRoot

# Locate go, tolerating a fresh Go install that is not on this shell's PATH.
$go = Get-Command go -ErrorAction SilentlyContinue
if ($go) {
    $go = $go.Source
} elseif (Test-Path "C:\Program Files\Go\bin\go.exe") {
    $go = "C:\Program Files\Go\bin\go.exe"
} else {
    throw "Go toolchain not found. Install Go 1.25+ (e.g. 'winget install GoLang.Go')."
}

if (-not $SkipTests) {
    Write-Host "Running tests..."
    & $go test ./...
    if ($LASTEXITCODE -ne 0) { throw "Tests failed; not installing." }
}

Write-Host "Building workgate.exe..."
& $go build -o (Join-Path $repo "workgate.exe") "$repo\cmd\workgate"
if ($LASTEXITCODE -ne 0) { throw "Build failed." }

$dest = Join-Path $env:LOCALAPPDATA "Programs\workgate"
New-Item -ItemType Directory -Force $dest | Out-Null
try {
    Copy-Item (Join-Path $repo "workgate.exe") $dest -Force
} catch {
    throw ("Could not overwrite $dest\workgate.exe - it is likely running. " +
        "Check 'workgate status', let active workloads finish, then re-run. ($_)")
}

$userPath = [Environment]::GetEnvironmentVariable("Path", "User")
if (($userPath -split ';') -notcontains $dest) {
    [Environment]::SetEnvironmentVariable("Path", ($userPath.TrimEnd(';') + ";" + $dest), "User")
    Write-Host "Added $dest to user PATH (new terminals will pick it up)."
}

Write-Host "Installed $dest\workgate.exe"
& (Join-Path $dest "workgate.exe") help | Select-Object -First 1
