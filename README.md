# 使用Python操作Hadoop，HDFS以及编写MapReduce程序

这学期在上《大数据技术》这门课，因为上课用的都是Java编写MapReduce程序，但平时Python用的比较多，于是研究了一下在Windows系统下，使用Python连接学校集群，并进行HDFS的的操作和MapReduce的编写，并记录一下踩的一些坑。

其中部分内容参考博客文章：[使用Python操作Hadoop，Python-MapReduce](https://segmentfault.com/a/1190000018781493)，若有侵权内容请联系删除。

## Python操作HDFS

首先简单介绍python操作HDFS的一些命令。

安装hdfs库：

```powershell
pip install hdfs
```

### 连接hdfs并查看文件

```python
from hdfs import *

# ref: https://blog.csdn.net/Gamer_gyt/article/details/52446757
client = Client('http://10.188.5.21:50070', root="/")
client.list('/')
```

### 创建目录

```python
client.makedirs('tmp/test_python_ma', permission=777)
```

### 删除目录

```python
client.delete('tmp/test_python_ma')
```

### 读取远程文件

```python
path = "/tmp/MaRelation/input"
input_txt = client.list(path)[0]
print(input_txt)
with client.read(path + '/' + input_txt, encoding='utf-8')as r:
    text = r.readlines()
for line in text:
    print(line.strip())
    print(line.strip()[:16])
```

读取文件可能会报如下错误：

```powershell
raise ConnectionError(e, request=request)
requests.exceptions.ConnectionError: HTTPConnectionPool(host='hadoop01.stu.com', port=50075): Max retries exceeded with url:
...
```

说明程序没有将主机名映射到正确的ip。需要在自己的电脑上的hosts文件中加上主机名和ip的映射，对于我所使用的windows系统，hosts文件的路径是```C://Windows/System32/drivers/etc/hosts```。从最后一行报错可以看到，主机名为“hadoop01.stu.com”，在文件末尾加上:

```powershell
10.188.5.21 hadoop01.stu.com
10.188.5.21 hadoop02.stu.com
10.188.5.21 hadoop03.stu.com
10.188.5.21 hadoop04.stu.com
```

因为学校的集群是4台电脑，所以01到04都需要添加（盲猜了一波猜对了）。

另外附上Windows修改hosts文件的方法：在hosts所在路径下左上角，文件 - 打开windows powershell：

```powershell
notepad hosts
```

其他的一些常见接口可以见参考文章。

## Python编写MapReduce程序

### 题目内容

给出一个child-parent的表格，要求从其中的父子辈关系，挖掘找出祖孙辈关系的表格。示例输入：（横线下面的行）

```textile
child           parent
-----------------------
Min Li          Qing Li
Min Li          Qi Yang
Jin Li          Qing Li
Jin Li          Qi Yang
Qing Li         XinXin Chen
Qing Li         Fu Li
Qi Yang         Lili Zhang
Qi Yang         Jie Wu
Daya Wu         Lili Zhang
Daya Wu         Jie Wu
Pin Wu          Daya Wu
Pin Wu          An Chen
Hang Wu         Daya Wu
Hang Wu         An Chen
```

示例输出文件内容如下：（横线下面的行）

```textile
grandchild      grandparent
-----------------------------
Min Li          Lili Zhang
Min Li          Jie Wu
Jin Li          Lili Zhang
Jin Li          Jie Wu
Min Li          XinXin Chen
Min Li          Fu Li
Jin Li          XinXin Chen
Jin Li          Fu Li
Pin Wu          Lili Zhang
Pin Wu          Jie Wu
Hang Wu         Lili Zhang
Hang Wu         Jie Wu
```

### 具体思路

参考文章：[【Mapreduce】利用单表关联在父子关系中求解爷孙关系](https://blog.csdn.net/yongh701/article/details/50622785)。下文中的图均来自此参考文章，这里介绍大体的思路。

1. 在Map阶段，把父 - 子关系和相反的子 - 父关系以键值对的方式进行提取，并在值之前加上一个标识符，能够识别出谁是父谁是子即可。具体做法参照图：
   
   ![原理](https://i.loli.net/2019/05/23/5ce69f20b09dc23211.png)

2. Reduce阶段前会经过一个shuffle，把key相同的放在同一个reduce任务中。在同个key的value数组中，我们可以根据前缀获得爷孙关系。

### Map

这里根据文件的格式做了些处理，比如跳过前两行，还有因为名字第二列是对齐的，根据观察第一个名字加上空格共是16个字符，所以做了下面的处理，大家根据实际情况就好了。

```python
import sys

for line in sys.stdin:
    child = line[:16].strip()
    parent = line[16:].strip()
    if child == 'child':  # skip the first row
        continue
    elif '-' in child or '-' in parent:  # skip the second row '--'
        continue
    else:
        print('\t'.join([child, 'p'+parent]))
        print('\t'.join([parent, 'c'+child]))
```

### Reduce

查了很多示例，好像在python中得自己判断key是否相同进行处理。所以待会在本地测试的时候必须在shell中用sort命令，对key进行排序，这样程序才能正确执行。

```python
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
```

测试文本```ex.txt```见上方。本地测试执行map：

```powershell
cat ex.txt | python rel_map.py
```

结果：

```powershell
Min Li  pQing Li
Qing Li cMin Li
Min Li  pQi Yang
Qi Yang cMin Li
Jin Li  pQing Li
Qing Li cJin Li
Jin Li  pQi Yang
Qi Yang cJin Li
Qing Li pXinXin Chen
XinXin Chen     cQing Li
Qing Li pFu Li
Fu Li   cQing Li
Qi Yang pLili Zhang
Lili Zhang      cQi Yang
Qi Yang pJie Wu
Jie Wu  cQi Yang
Daya Wu pLili Zhang
Lili Zhang      cDaya Wu
Daya Wu pJie Wu
Jie Wu  cDaya Wu
Pin Wu  pDaya Wu
Daya Wu cPin Wu
Pin Wu  pAn Chen
An Chen cPin Wu
Hang Wu pDaya Wu
Daya Wu cHang Wu
Hang Wu pAn Chen
An Chen cHang Wu
```

本地测试MapReduce代码：

```powershell
cat ex.txt | python rel_map.py | sort -k 1r | python rel_reduce.py 
```

其中```sort -k 1r```是进行倒序。结果：

```powershell
grandchild      grandparent
-----------------------------
Min Li          Lili Zhang
Min Li          Jie Wu
Jin Li          Lili Zhang
Jin Li          Jie Wu
Min Li          XinXin Chen
Min Li          Fu Li
Jin Li          XinXin Chen
Jin Li          Fu Li
Pin Wu          Lili Zhang
Pin Wu          Jie Wu
Hang Wu         Lili Zhang
Hang Wu         Jie Wu
```

### 在Hadoop上执行MapReduce程序

mapreduce的介绍：[[http://dblab.xmu.edu.cn/blog/hadoop-build-project-using-eclipse/](http://dblab.xmu.edu.cn/blog/hadoop-build-project-using-eclipse/)]([http://dblab.xmu.edu.cn/blog/hadoop-build-project-using-eclipse/](http://dblab.xmu.edu.cn/blog/hadoop-build-project-using-eclipse/))。

首先在hdfs上创建文件夹：

```powershell
hadoop dfs -mkdir /relation
```

若创建时出现permission denied，则在hdfs-site.xml里加上：

```powershell
<property>
       <name>dfs.permissions</name>
       <value>false</value>
</property>
```

上传文件：

```powershell
hadoop dfs -put ex.txt /relation
```

Hadoop虽然基于Java，但它有专门的jar包来提供解析我们自己编写的python MapReduce程序。根据自己电脑配置的路径：

```powershell
hadoop jar /usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-3.2.0.jar -file ./rel_map.py -file ./rel_reduce.py -input /relation -output /relation_output -mapper "python rel_map.py" -reducer "python rel_reduce.py" 
```

执行结果：

```powershell
hadoop dfs -cat /relation_output/*
```

```powershell
grandchild      grandparent    
-----------------------------    
Pin Wu          Lili Zhang    
Pin Wu          Jie Wu    
Hang Wu         Lili Zhang    
Hang Wu         Jie Wu    
Jin Li          Lili Zhang    
Jin Li          Jie Wu    
Min Li          Lili Zhang    
Min Li          Jie Wu    
Jin Li          XinXin Chen    
Jin Li          Fu Li    
Min Li          XinXin Chen    
Min Li          Fu Li    
```
