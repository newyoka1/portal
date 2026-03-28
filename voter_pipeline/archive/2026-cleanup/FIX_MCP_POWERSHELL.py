"""Fix Windows-MCP PowerShell path issue.
The uv venv strips PATH so 'powershell' can't be found.
This replaces it with the full path to powershell.exe.
"""
import os

svc = os.path.expandvars(
    r"%APPDATA%\Claude\Claude Extensions\ant.dir.cursortouch.windows-mcp\src\windows_mcp\desktop\service.py"
)

with open(svc, "r", encoding="utf-8") as f:
    src = f.read()

old = '''                    "powershell",'''
new = '''                    r"C:\\Windows\\System32\\WindowsPowerShell\\v1.0\\powershell.exe",'''

if old not in src:
    if "powershell.exe" in src:
        print("Already patched! PowerShell path is already set to full path.")
    else:
        print("ERROR: Could not find the target string to replace.")
    input("Press Enter to close...")
    raise SystemExit

# Backup
bak = svc + ".bak"
if not os.path.exists(bak):
    with open(bak, "w", encoding="utf-8") as f:
        f.write(src)
    print(f"Backup: {bak}")

src = src.replace(old, new, 1)

with open(svc, "w", encoding="utf-8") as f:
    f.write(src)

print(f"FIXED: {svc}")
print()
print('Changed "powershell" -> full path to powershell.exe')
print()
print("Now restart the Windows-MCP server:")
print("  1. In Claude desktop app, go to Settings > MCP")
print("  2. Toggle Windows-MCP OFF then ON")
print("  3. Or just restart the Claude app")
input("\nPress Enter to close...")
