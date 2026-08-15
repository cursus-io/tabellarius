param(
    [switch]$KeepRunning
)

$ErrorActionPreference = "Stop"
$composeFile = "test/docker-compose.yaml"

function Invoke-Compose {
    param([string[]]$ComposeArgs)

    & docker compose -f $composeFile @ComposeArgs
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose $($ComposeArgs -join ' ') failed"
    }
}

function Wait-ForContainer {
    param([string]$Name, [string]$ExpectedStatus)

    for ($attempt = 1; $attempt -le 30; $attempt++) {
        $status = (& docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' $Name 2>$null).Trim()
        if ($status -eq $ExpectedStatus) {
            return
        }
        Start-Sleep -Seconds 2
    }

    throw "$Name did not reach status $ExpectedStatus"
}

function Initialize-Cdc {
    for ($attempt = 1; $attempt -le 10; $attempt++) {
        & docker compose -f $composeFile run --rm cdc-cli --mode init --apply=true
        if ($LASTEXITCODE -eq 0) {
            return
        }
        Start-Sleep -Seconds 2
    }

    throw "cdc-cli initialization did not succeed"
}

function Add-User {
    param([string]$Suffix)

    $email = "stability-$Suffix@example.com"
    $sql = "INSERT INTO users (email, name) VALUES ('$email', '$Suffix')"
    Invoke-Compose -ComposeArgs @('exec', '-T', 'mysql', 'mysql', '-uroot', '-proot', 'mydb', '-e', $sql)
}

function Assert-PublishAfter {
    param([datetime]$Since, [string]$Scenario)

    $sinceText = $Since.ToUniversalTime().ToString("o")
    for ($attempt = 1; $attempt -le 30; $attempt++) {
        $logs = & docker compose -f $composeFile logs --since $sinceText cdc-server
        if ($logs -match "\[publish\].*table=mydb\.users.*op=INSERT") {
            return
        }
        Start-Sleep -Seconds 2
    }

    throw "$Scenario did not publish a new user transaction"
}

try {
    Invoke-Compose -ComposeArgs @('up', '-d', '--build', 'broker', 'mysql')
    Wait-ForContainer "cdc-mysql" "healthy"
    Wait-ForContainer "tabellarius-cursus" "healthy"
    Initialize-Cdc
    Invoke-Compose -ComposeArgs @('up', '-d', 'cdc-server')

    $serverRestartAt = Get-Date
    Invoke-Compose -ComposeArgs @('restart', 'cdc-server')
    Add-User ([guid]::NewGuid().ToString("N"))
    Assert-PublishAfter $serverRestartAt "CDC server restart recovery"

    $brokerRestartAt = Get-Date
    Invoke-Compose -ComposeArgs @('restart', 'broker')
    Wait-ForContainer "tabellarius-cursus" "healthy"
    Add-User ([guid]::NewGuid().ToString("N"))
    Assert-PublishAfter $brokerRestartAt "Cursus broker restart recovery"

    $env:CURSUS_ADDR = "127.0.0.1:9000"
    go test -tags integration ./pkg/source/cursus -run TestCursusTopicContainsPublishedCDCRecords -count=1
    if ($LASTEXITCODE -ne 0) {
        throw "Cursus topic offset verification failed"
    }

    Write-Host "Cursus stability E2E passed."
}
finally {
    if (-not $KeepRunning) {
        & docker compose -f $composeFile down
    }
}
