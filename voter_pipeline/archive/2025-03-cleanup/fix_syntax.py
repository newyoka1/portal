with open(r'D:\git\nys-voter-pipeline\donors\boe_match_aggregate.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Fix the malformed print statement - replace literal newline with \n escape
import re
content = re.sub(r'print\("[\r\n\s]+Step 0:', r'print("\\nStep 0:', content)

with open(r'D:\git\nys-voter-pipeline\donors\boe_match_aggregate.py', 'w', encoding='utf-8') as f:
    f.write(content)

print('Fixed syntax error!')
