import sys

for line in sys.stdin:
    child = line[:16].strip()
    parent = line[16:].strip()
    if child == 'child':
        continue
    elif '-' in child or '-' in parent:
        continue
    else:
        print('\t'.join([child, 'p'+parent]))
        print('\t'.join([parent, 'c'+child]))