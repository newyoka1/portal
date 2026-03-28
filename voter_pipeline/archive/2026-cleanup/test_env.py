import os, sys
os.chdir(r"D:\git\nys-voter-pipeline")
sys.path.insert(0, r"D:\git\nys-voter-pipeline")
try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

out = []
out.append(f"AIVEN_HOST={os.environ.get('AIVEN_HOST','NOT SET')}")
out.append(f"AIVEN_PORT={os.environ.get('AIVEN_PORT','NOT SET')}")
out.append(f"AIVEN_USER={os.environ.get('AIVEN_USER','NOT SET')}")
out.append(f"AIVEN_DB={os.environ.get('AIVEN_DB','NOT SET')}")
out.append(f"AIVEN_SSL_CA={os.environ.get('AIVEN_SSL_CA','NOT SET')}")
out.append(f"PW_SET={'yes' if os.environ.get('AIVEN_PASSWORD') else 'NO'}")

with open(r"D:\git\nys-voter-pipeline\test_out.txt","w") as f:
    f.write("\n".join(out))
