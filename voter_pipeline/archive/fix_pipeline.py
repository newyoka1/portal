path = r"D:\git\nys-voter-pipeline\pipeline\pipeline.py"
with open(path, "r", encoding="utf-8") as f:
    lines = f.readlines()

# Find the bad block and replace it
new_block = """# Load environment variables from .env file
try:
    from dotenv import load_dotenv
    env_path = Path(__file__).parent.parent / ".env"
    if not env_path.exists():
        env_path = Path(__file__).parent / ".env"
    if env_path.exists():
        load_dotenv(env_path)
except ImportError:
    pass

# =========================
# CONFIG
# =========================
DB_NAME = "nys_voter_tagging"

MYSQL_HOST     = os.getenv("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT     = int(os.getenv("MYSQL_PORT", "3306"))
MYSQL_USER     = os.getenv("MYSQL_USER", "root")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD", "")

if not MYSQL_PASSWORD:
    raise ValueError("MYSQL_PASSWORD not set - check your .env file")

"""

# Find start of bad block
start = None
end = None
for i, line in enumerate(lines):
    if start is None and "# Load environment variables from .env file" in line:
        start = i
    if start is not None and i > start and "raise ValueError" in line:
        end = i + 1
        break

print(f"Replacing lines {start} to {end}")
new_lines = lines[:start] + [new_block] + lines[end:]

with open(path, "w", encoding="utf-8") as f:
    f.writelines(new_lines)

# Verify
with open(path, "r", encoding="utf-8") as f:
    content = f.read()
print("from dotenv import load_dotenv:", "from dotenv import load_dotenv" in content)
print("MYSQL_PASSWORD = os.getenv:", "MYSQL_PASSWORD = os.getenv" in content)
print("DB_NAME = nys_voter_tagging:", 'DB_NAME = "nys_voter_tagging"' in content)