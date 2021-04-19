

### 关系模型到 Key-Value 模型的映射

lab1主要解决如何在 KV 结构上保存 Table 。

表前缀t

行前缀_r

索引前缀_i

映射关系如下

> RecordRowKeyLen = 1     +      8           +     2               +         8
>
> ​								      t            idlen			_r                       handle



> RecordRowKeyLen = 1     +      8           +     2               +         8           +          encodedValue...
>
> ​								   （ t            idlen			_i）                   idxID        +		encodedValue...
>
> ​									appendTableIndexPrefix                    idxID                         []byte

详见官网的例:

```fallback
t10_r1 --> ["TiDB", "SQL Layer", 10]
t10_r2 --> ["TiKV", "KV Engine", 20]
t10_r3 --> ["PD", "Manager", 30]
```

除了 Primary Key 之外，这个表还有一个 Index，假设这个 Index 的 ID 为 1，则其数据为：

```fallback
t10_i1_10_1 --> null
t10_i1_20_2 --> null
t10_i1_30_3 --> null
```

有了上述描述,就很容易理解一下两个编码函数了。![image-20210419190619093](https://tva1.sinaimg.cn/large/008eGmZEgy1gpp9fibwe5j31i608awj2.jpg)

---



### 字节序

<img src="/Users/lonekriss/Library/Application Support/typora-user-images/image-20210418221638356.png" alt="image-20210418221638356" style="zoom:50%;" />



先来理解下字节序。

将不同数据结构转换为字节序，方便存储传输。同时也不用考虑类型影响。

第一行可以看到地址由低向高。

采用大端保存字节序，阅读顺序就与人阅读顺序一致。高地址储存低字节。

来简单理解序列化与反序列化。以下为主要实现摘抄。

```go
package main

import (
	"fmt"
	"encoding/binary"

)

const signMask uint64 = 0x8000000000000000

var (
	tablePrefix     = []byte{'t'}
	recordPrefixSep = []byte("_r")
	indexPrefixSep  = []byte("_i")
)

func EncodeIntToCmpUint(v int64) uint64 {
	return uint64(v) ^ signMask  
}

func DecodeCmpUintToInt(u uint64) int64 {
	return int64(u ^ signMask)
}

func EncodeInt(b []byte, v int64) []byte {
	var data [8]byte
	u := EncodeIntToCmpUint(v)
	binary.BigEndian.PutUint64(data[:], u)
	return append(b, data[:]...)
}

func DecodeInt(b []byte) ([]byte, int64) {
	if len(b) < 8 {
		return nil, 0
	}

	u := binary.BigEndian.Uint64(b[:8])
	v := DecodeCmpUintToInt(u)
	b = b[8:]
	return b, v
}

func main() {
	var tableID int64
	tableID = 1
	buf := make([]byte, 0, 11)
	buf = append(buf, tablePrefix...)
	buf = EncodeInt(buf, tableID)
	buf = append(buf, recordPrefixSep...)
	fmt.Println(uint64(tableID))
	fmt.Println(buf)
	fmt.Println(string(buf[0:1]))
	u,v:=DecodeInt(buf[1:9])
	fmt.Println(u,v)
	
}

```

可以从上图看出序列化后的字节流。

所以我们需要做的事就是按照字节流格式，decode出我们需要的字段。

---



**疑问**：

为什么需要EncodeIntToCmpUint和DecodeCmpUintToInt？  

为什么要解码成uin64t然后转换为int64？      （go没有int64转大端的函数？）

**异或操作具体是为了解决或实现哪个目的？**

-----

## 理解代码

tablecodec 的主要代码位于 [tablecodec.go](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go)，这次我们需要关注的代码主要从 [L33 到 L147](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L33-L146) 之间。

代码一开始，定义了上文提到的三个常量：tablePrefix，recordPrefixSep 和 indexPrefixSep。

接下来可以看到 [EncodeRowKeyWithHandle](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L64) 和 [EncodeIndexSeekKey](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L86) 分别实现了上述所说的行数据和索引数据的编码。

## 作业描述

根据上述 [EncodeRowKeyWithHandle](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L64) 和 [EncodeIndexSeekKey](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L86)，实现 [DecodeRecordKey](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L72) 和 [DecodeIndexKeyPrefix](https://github.com/pingcap-incubator/tinysql/blob/course/tablecodec/tablecodec.go#L95)，注意由于参数 `key` 可能是不合法的，需要考虑错误处理。



一开始认为需要自己写实现，结果阅读代码后发现里面把需要的功能封装好了。

不过阅读的不是很细致，只是分析了encode和decode用到的主要函数。

recor

![image-20210419184831435](https://tva1.sinaimg.cn/large/008eGmZEgy1gpp8x0m59ij317y01qt96.jpg)

index

![image-20210419205056982](/Users/lonekriss/Library/Application Support/typora-user-images/image-20210419205056982.png)

