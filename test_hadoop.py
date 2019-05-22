from hdfs import *

# ref: https://blog.csdn.net/Gamer_gyt/article/details/52446757
client = Client('http://10.188.5.21:50070', root="/")
client.makedirs('tmp/test_python_ma', permission=777)
# client.delete('tmp/test_python')
print(client.list("/tmp"))

path = "/tmp/MaRelation/input"
input_txt = client.list(path)[0]
print(input_txt)
with client.read(path + '/' + input_txt, encoding='utf-8')as r:
    text = r.readlines()
for line in text:
    print(line.strip())
    print(line.strip()[:16])

#
# for i in range(2, 16):
#     print(text[i].strip())

