from pathlib import Path
p=Path('main.py')
lines=p.read_text(encoding='utf-8').splitlines()
ln=988
print('Line',ln)
print(repr(lines[ln-1]))
print('Prev lines:')
for i in range(ln-5, ln+5):
    print(i, repr(lines[i-1]))
