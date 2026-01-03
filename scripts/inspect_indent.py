from pathlib import Path
p=Path('main.py')
lines=p.read_text(encoding='utf-8').splitlines()
for i in range(990, 1020):
    l = lines[i-1]
    indent = len(l) - len(l.lstrip(' '))
    print(f"{i:4} indent={indent}: {repr(l)}")
