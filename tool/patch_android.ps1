param(
    [Parameter(Mandatory=$true)][string]$FlutterRoot,
    [Parameter(Mandatory=$true)][string]$PubCache
)
$ErrorActionPreference = 'Stop'
$RepoRoot = Split-Path $PSScriptRoot -Parent
$FlutterRoot = (Resolve-Path -LiteralPath $FlutterRoot).Path
$PubCache = (Resolve-Path -LiteralPath $PubCache).Path

# The same Android patches as lib/scripts/patch.ps1, without global git
# configuration, hard reset, or deleting a shared package cache.
function Apply-Patches([string]$Target, [string[]]$Names, [string]$Prefix) {
    foreach ($Name in $Names) {
        $Patch = Join-Path $RepoRoot "lib/scripts/$Prefix$Name.patch"
        & git -c "safe.directory=$Target" -C $Target apply --check $Patch 2>$null
        if ($LASTEXITCODE -eq 0) {
            & git -c "safe.directory=$Target" -C $Target apply $Patch
            if ($LASTEXITCODE -ne 0) { throw "Cannot apply $Name" }
        } else {
            & git -c "safe.directory=$Target" -C $Target apply --reverse --check $Patch 2>$null
            if ($LASTEXITCODE -ne 0) { throw "Patch conflicts: $Name in $Target" }
        }
        Write-Output "Ready: $Name"
    }
}
Apply-Patches $FlutterRoot @(
    'modal_barrier', 'text_selection', 'mouse_cursor', 'image_anim',
    'layout_builder', 'navigation_drawer', 'popup_menu', 'fab',
    'null_safety_for_selectable_region', 'selectable_region', 'editable_text',
    'text_field', 'scroll_position', 'scrollable', 'scrollable_gesture',
    'draggable_scrollable_sheet', 'scaffold', 'text', 'text_painter', 'sliver',
    'refresh_indicator', 'bottom_sheet_android', 'scroll_view', 'navigator'
) ''
$Config = Get-Content (Join-Path $RepoRoot '.dart_tool/package_config.json') -Raw | ConvertFrom-Json
$Material = $Config.packages | Where-Object name -eq 'material_ui'
if (-not $Material -or -not $Material.rootUri.StartsWith('file:')) { throw 'Run flutter pub get first' }
$MaterialPath = ([Uri]$Material.rootUri).LocalPath.TrimEnd([IO.Path]::DirectorySeparatorChar)
if (-not $MaterialPath.StartsWith($PubCache + [IO.Path]::DirectorySeparatorChar, [StringComparison]::OrdinalIgnoreCase)) {
    throw 'material_ui is outside the specified PubCache'
}
Apply-Patches $MaterialPath @(
    'modal_barrier_material', 'navigation_drawer', 'popup_menu', 'fab',
    'text_field', 'scaffold', 'refresh_indicator', 'tabs', 'bottom_sheet_android'
) 'material/'
