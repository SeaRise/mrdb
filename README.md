mrdb是一个多线程nosql数据库,这个只是用来练手完成的,实现了读可提交和可重复读两种隔离度,实现了并发的b+树,即blink树.
一共六个模块:
- tbm表管理
- vm多版本控制
- tm事务管理
- im索引管理
- dm底层数据存储管理,因为这个数据库只是练手用的,所以dm的数据容量只有256KB.
- util提供序列化的方法和byte[]复用

模块依赖关系:
```
|-----|     |-----|
| tbm |-----| im  |
|-----|     |-----|
   |	       |
   |           |
   |           |
|-----|     |-----|
| vm  |-----| dm  |
|-----|     |-----|
   |           |
   |           |
   |           |
|-----|        |
| tm  |--------|
|-----|
```

- dm:提供了dm数据管理,用分页管理实现了数据缓存,读写用byte[].每个对dm的写操作都记录在日志,
每次启动dm都会重放日志里的操作.dm可以保证操作的原子性和持久性
- vm:实现了mvcc,为每个数据项提供了xmin和xmax,用来进行多版本控制.提供可重复读和读已提交两种隔离度.
- tm:进行事务管理,因为用了mvcc所以dm不提供delete操作,发生回滚时只需更新tm中相应事务的状态,
在vm模块进行可见性判断时就会对相应的事务返回false,相当于撤回了.所以每次启动dm重放日志的时候会检测是否需要回滚,
回滚只需用tm更新事务的状态即可.
- im:提供了并发的b+树,节点存储在dm,借助dm本身的原子性保证节点自身结构不会被破坏,
根据blink树协议保证节点间关系在并发读写下不会被破坏,且不会发生死锁.
- util:提供序列化的方法和byte[]复用,每次使用byte[]都从util取,用完放回util,避免重复new byte[],频繁触发GC,而且容易产生朝生夕灭的大数组.


测试这一块没怎么做,所以这个nosql可能还有很多bug.....先这样把.
