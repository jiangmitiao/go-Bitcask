# go-Bitcask
an instance of Bitcask Storage Model 


Bitcask是一个日志型的基于hash表结构和key-value存储模型,何谓日志型？就是append only，所有写操作只追加而不修改老的数据，就像我们的各种服务器日志一样。在Bitcask模型中，数据文件以日志型只增不减的写入文件，而文件有一定的大小限制，当文件大小增加到相应的限制时，就会产生一个新的文件，老的文件将只读不写。在任意时间点，只有一个文件是可写的，在Bitcask模型中称其为active data file，而其他的已经达到限制大小的文件，称为older data file 。

这是一个由go语言编写的bitcask模型实例。