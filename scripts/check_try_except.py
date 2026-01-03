import re
p = r"c:\\Users\\nervblyati\\Desktop\\market\\main.py"
with open(p, 'r', encoding='utf-8') as f:
    lines = f.readlines()
tries = []
excepts = []
for i,l in enumerate(lines, start=1):
    if re.search(r"^\s*try:\s*$", l):
        tries.append((i, len(l) - len(l.lstrip(' '))))
    if re.search(r"^\s*except\b", l) or re.search(r"^\s*finally\b", l):
        excepts.append((i, len(l) - len(l.lstrip(' '))))
print('try count', len(tries))
print('except/finally count', len(excepts))
print('try lines (line,indent):', tries)
print('except lines (line,indent):', excepts)

# Match tries with nearest except/finally at same indent
stack = []
unmatched = []
for i,l in enumerate(lines, start=1):
    stripped = l.lstrip(' ')
    indent = len(l) - len(stripped)
    if re.match(r"^try:\s*$", stripped):
        stack.append((i, indent))
    if re.match(r"^(except\b|finally\b)", stripped):
        # pop the most recent try with same indent
        for j in range(len(stack)-1, -1, -1):
            if stack[j][1] == indent:
                stack.pop(j)
                break

if stack:
    print('Unmatched try blocks (line,indent):', stack)
else:
    print('All try blocks matched with except/finally (by indent).')
# show mismatched region: print around first try with no nearby except
for t in tries:
    # find except after this try
    found = False
    for e in excepts:
        if e > t:
            found = True
            break
    if not found:
        print('unmatched try at', t)
        break
print('\n--- snippet around error lines ---')
for n in range(max(1,1000), 1030):
    if n<=len(lines):
        print(f"{n:4}: {lines[n-1].rstrip()}")
