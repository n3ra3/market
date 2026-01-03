from pathlib import Path
lines=Path('main.py').read_text(encoding='utf-8').splitlines()
for idx in (988,1008,1016,1019):
    l=lines[idx-1]
    print(idx, len(l)-len(l.lstrip(' ')), repr(l))
for idx in (1009,1010):
    l=lines[idx-1]
    print(idx, len(l)-len(l.lstrip(' ')), repr(l))
