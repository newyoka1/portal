# Quick fix for syntax error
$file = "D:\git\nys-voter-pipeline\donors\boe_match_aggregate.py"
$content = Get-Content $file -Raw
$content = $content -replace 'print\("[\r\n]+Step 0:', 'print("`nStep 0:'
$content | Set-Content $file -NoNewline
Write-Host "Fixed!"
