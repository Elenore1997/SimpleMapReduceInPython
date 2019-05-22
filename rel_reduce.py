import sys

grandparent = []
grandchild = []
cur_key = None
print('grandchild' + ' '*(16-len('grandchild')) + 'grandparent')
print('-' * 29)
for line in sys.stdin:
    ss = line.strip().split('\t')

    if len(ss) < 2:
        continue

    key = ss[0]
    value = ss[1]

    if cur_key == None:
        cur_key = key

    if cur_key != key:
        for i in range(len(grandchild)):
            for j in range(len(grandparent)):
                # print('\t'.join([grandchild[i], grandparent[j]]))
                print(grandchild[i] + ' '*(16-len(grandchild[i])) + grandparent[j])
        cur_key = key
        grandparent = []
        grandchild = []

    if value[0] == 'p':
        if value[1:] not in grandparent:
            grandparent.append(value[1:])
    else:
        if value[1:] not in grandchild:
            grandchild.append(value[1:])

for i in range(len(grandchild)):
    for j in range(len(grandparent)):
        # print('\t'.join([grandchild[i], grandparent[j]]))
        print(grandchild[i] + ' ' * (16 - len(grandchild[i])) + grandparent[j])

'''
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.0.jar -file ./rel_map.py -file ./rel_reduce.py -input /relation -output /relation_output -mapper "python rel_map.py" -reducer "python rel_reduce.py" 
'''