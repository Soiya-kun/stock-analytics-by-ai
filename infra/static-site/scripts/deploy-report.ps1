param(
    [Parameter(Mandatory = $true)][switch]$HistoryReader,
    [string]$Profile = '373117960731_AdministratorAccess',
    [string]$Region = 'ap-northeast-1'
)
$ErrorActionPreference = 'Stop'
if (!$HistoryReader) { throw 'Specify -HistoryReader to publish the stock history reader' }
& (Join-Path $PSScriptRoot 'deploy-history-reader.ps1') -Profile $Profile -Region $Region
