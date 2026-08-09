$ErrorActionPreference = "Stop"
$base = "http://localhost:8080"

Write-Host "Checking health..."
Invoke-RestMethod "$base/healthz" | ConvertTo-Json

Write-Host "Creating agent..."
$agent = Invoke-RestMethod -Method Post -Uri "$base/api/v1/agents" -ContentType "application/json" -Body (@{
    name = "Researcher"
    system_prompt = "Be concise and evidence-oriented."
} | ConvertTo-Json)
$agent | ConvertTo-Json

Write-Host "Creating run..."
$run = Invoke-RestMethod -Method Post -Uri "$base/api/v1/runs" -ContentType "application/json" -Body (@{
    agent_id = $agent.id
    input = "Explain why Go is useful for control planes."
} | ConvertTo-Json)
$run | ConvertTo-Json

Start-Sleep -Seconds 2
Write-Host "Fetching run..."
Invoke-RestMethod "$base/api/v1/runs/$($run.id)" | ConvertTo-Json
