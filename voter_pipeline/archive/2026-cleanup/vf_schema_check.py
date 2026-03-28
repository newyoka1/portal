import mysql.connector, sys

out = []
try:
    conn = mysql.connector.connect(host="127.0.0.1", port=3306, user="root", password="!#goAmerica99")
    cur = conn.cursor()
    cur.execute("SET SESSION sql_mode=''")

    cur.execute("DESCRIBE nys_voter_tagging.voter_file")
    out.append("=== voter_file SCHEMA ===")
    for r in cur.fetchall():
        out.append(str(r))

    cur.execute("SELECT * FROM nys_voter_tagging.voter_file LIMIT 3")
    out.append("=== voter_file SAMPLE ===")
    cols = [d[0] for d in cur.description]
    out.append(str(cols))
    for r in cur.fetchall():
        out.append(str(r))

    conn.close()
except Exception as e:
    out.append(f"ERROR: {e}")
    import traceback
    out.append(traceback.format_exc())

with open(r"D:\git\nys-voter-pipeline\vf_schema_out.txt", "w") as f:
    f.write("\n".join(out))
