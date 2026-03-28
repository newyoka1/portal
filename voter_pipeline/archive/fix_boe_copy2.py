import sys, os, time
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from utils.db import get_conn

log_path = r"D:\git\nys-voter-pipeline\logs\boe_copy.log"

def log(msg):
    line = f"[{time.strftime('%H:%M:%S')}] {msg}"
    print(line, flush=True)
    with open(log_path, "a") as f:
        f.write(line + "\n")

# Clear log
open(log_path, "w").close()

log("Connecting (timeout=3600)...")
conn = get_conn("nys_voter_tagging", autocommit=True, timeout=3600)
cur = conn.cursor()

log("Dropping empty boe_contributions_raw from nys_voter_tagging...")
cur.execute("DROP TABLE IF EXISTS `nys_voter_tagging`.`boe_contributions_raw`")
log("Dropped.")

log("Creating structure...")
cur.execute("CREATE TABLE `nys_voter_tagging`.`boe_contributions_raw` LIKE `donors_2024`.`boe_contributions_raw`")
log("Structure created.")

log("Starting INSERT of 3.3M rows...")
t = time.time()
cur.execute("INSERT INTO `nys_voter_tagging`.`boe_contributions_raw` SELECT * FROM `donors_2024`.`boe_contributions_raw`")
elapsed = time.time() - t

cur.execute("SELECT COUNT(*) FROM `nys_voter_tagging`.`boe_contributions_raw`")
tgt = cur.fetchone()[0]
cur.execute("SELECT COUNT(*) FROM `donors_2024`.`boe_contributions_raw`")
src = cur.fetchone()[0]

log(f"Finished in {elapsed:.1f}s  src={src:,}  tgt={tgt:,}  match={'YES' if src==tgt else 'MISMATCH'}")
cur.close(); conn.close()