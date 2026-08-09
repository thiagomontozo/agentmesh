$ErrorActionPreference = "Stop"
Set-Location (Split-Path -Parent $PSScriptRoot)
go run ./cmd/agentmesh
