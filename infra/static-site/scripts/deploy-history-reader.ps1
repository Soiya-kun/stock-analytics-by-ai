param(
    [ValidateSet('285A','4062')][string]$SecurityCode = '285A',
    [string]$Profile = '373117960731_AdministratorAccess',
    [string]$Region = 'ap-northeast-1'
)
$ErrorActionPreference = 'Stop'
$previousRegion = $env:STACK_REGION
$previousCode = $env:READER_SC
Push-Location (Join-Path $PSScriptRoot '..')
try {
    $env:STACK_REGION = $Region
    $env:READER_SC = $SecurityCode
    $assembly = if ($SecurityCode -eq '285A') { 'cdk.reader.out' } else { "cdk.reader.$SecurityCode.out" }
    npm run build
    if ($LASTEXITCODE -ne 0) { throw 'TypeScript build failed' }
    $app = 'npx ts-node --prefer-ts-exts bin/history-reader.ts'
    npx cdk synth --app $app --output $assembly --profile $Profile --quiet
    if ($LASTEXITCODE -ne 0) { throw 'Reader synthesis failed' }
    $identityJson = aws sts get-caller-identity --profile $Profile --output json
    if ($LASTEXITCODE -ne 0) { throw "AWS login required: aws sso login --profile $Profile" }
    if (($identityJson | ConvertFrom-Json).Account -ne '373117960731') { throw 'Unexpected AWS account' }
    $stackJson = aws cloudformation describe-stacks --stack-name StockAnalyticsStaticSite --profile $Profile --region $Region --output json
    if ($LASTEXITCODE -ne 0) { throw 'Cannot read existing static site stack' }
    $resourcesJson = aws cloudformation list-stack-resources --stack-name StockAnalyticsStaticSite --profile $Profile --region $Region --output json
    if ($LASTEXITCODE -ne 0) { throw 'Cannot read existing site resources' }
    $outputs = ($stackJson | ConvertFrom-Json).Stacks[0].Outputs
    $bucket = ($outputs | Where-Object OutputKey -eq 'BucketName').OutputValue
    $siteUrl = ($outputs | Where-Object OutputKey -eq 'SiteUrl').OutputValue
    $distributions = @(($resourcesJson | ConvertFrom-Json).StackResourceSummaries | Where-Object ResourceType -eq 'AWS::CloudFront::Distribution')
    if (!$bucket -or !$siteUrl -or $distributions.Count -ne 1) { throw 'Existing site outputs are incomplete or ambiguous' }
    $siteDomain = ([uri]$siteUrl).Host
    $distribution = $distributions[0].PhysicalResourceId
    npx cdk diff --app $assembly --profile $Profile --no-change-set
    if ($LASTEXITCODE -ne 0) { throw 'CDK diff failed' }
    $deployArgs = @('--app', $assembly, '--profile', $Profile,
        '--parameters', "SiteBucketName=$bucket", '--parameters', "SiteDistributionId=$distribution",
        '--parameters', "SiteDomainName=$siteDomain")
    npx cdk deploy @deployArgs --require-approval never --outputs-file "$assembly/deployed-outputs.json"
    if ($LASTEXITCODE -ne 0) { throw 'CDK deployment failed' }
    $deployed = (Get-Content -LiteralPath "$assembly/deployed-outputs.json" -Raw | ConvertFrom-Json)."StockAnalyticsHistoryReader$SecurityCode"
    $verified = $false
    for ($attempt = 0; $attempt -lt 12; $attempt++) {
        $download = [System.IO.Path]::GetTempFileName()
        try {
            curl.exe --fail --silent --show-error --max-time 30 --output $download $deployed.ReportUrl
            if ($LASTEXITCODE -ne 0) { throw 'HTTPS download failed' }
            $actual = (Get-FileHash -LiteralPath $download -Algorithm SHA256).Hash.ToLowerInvariant()
            if ($actual -eq $deployed.ReportSha256) { $verified = $true; break }
        } catch { Write-Host "HTTPS verification retry $($attempt + 1): $($_.Exception.Message)" }
        finally { Remove-Item -LiteralPath $download -Force }
        Start-Sleep -Seconds 5
    }
    if (!$verified) { throw "Published HTML hash not yet verified at $($deployed.ReportUrl)" }
    Write-Host "Verified history reader: $($deployed.ReportUrl)"
} finally {
    $env:STACK_REGION = $previousRegion
    $env:READER_SC = $previousCode
    Pop-Location
}
