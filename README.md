# redis7.0 中文注释
## 项目介绍：
大家在学习一个热门开源项目的源码的时候，是不是经常会感到不解？或许你是英语不好导致的，也可能是没有注释又无法理解代码意思导致的。这是否令你常常想要放弃通过阅读源码去学习一个项目呢？从今天开始答应我不要再放弃了，好吗？因为 redis7.0 中文注释它来了！🎆🎇✨🎉<br>
本项目旨在帮助中文区的大家能够更容易的去阅读，学习 redis 源码🍭🍭<br>
在你学习 redis 源码的同时，如果你理解了一段源码，我们也希望你可以将你的理解做成中文注释并向本仓库发起 pull request 来和大家分享你的想法。因为让一个人来完成这么大的工程是极其困难的，所以本项目的开源目的也是想让大家一起来完善本仓库的源码注释。人多力量大！争取达成所有源码都有中文注释的目标！同时让大家都参与源码注释的修改工作，我们的注释也能够集百家之所长！🎉🎉<br>
### 本项目很可能会成为你第一个参与的开源项目！
你只需要翻译源码里某个有较详细英文注释的函数，你就可以发起 PR 并成为 contributor！更好的是你还能带上你自己的理解。除此之外还有更简单的如：修改错别字、修改格式错误或者修改意思错误的注释，你也可以成为 contributor。该项目目前仍处于初期，还有很多未完成注释的源码，希望大家能够一起合力把注释填满整个项目！✨✨<br>
本项目灵感来源于《Redis设计与实现》作者黄健宏的redis3.0注释仓库：https://github.com/huangz1990/redis-3.0-annotated<br>
redis 仓库链接：https://github.com/redis/redis<br>
点击右上角的 star⭐，可以持续关注我们仓库接下来的更新哦!🍭🍭
## 项目进度：
完成度标准介绍：
<li>完成：文件中的每个有必要注释的结构体定义和每个函数都有中文注释。
<li>过半：文件中有中文注释的结构体定义和函数比例过半。
<li>低于一半：文件中有中文注释的结构体定义和函数比例低于一半。<p>


| 文件 | 作用 | 完成度 |
| - | :-: | - |
| [t_string.c ](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/t_string.c) | 定义了 string 字符串类型以及相关命令，例如 SET/GET | 完成 |
| [t_hash.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/t_hash.c) | 定义了 hash 哈希类型以及相关命令，例如 HSET/HGET | 完成 |
| [t_list.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/t_list.c) | 定义了 list 列表类型以及相关命令，例如 LPUSH/RPOP | 完成 |
| [t_set.c ](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/t_set.c ) | 定义了 set 集合类型以及相关命令，例如 SADD/SMEMBERS | 完成 |
| [t_zset.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/t_zset.c) | 定义了 zset 有序类型（包含 skiplist 跳表的实现）以及相关命令，例如 ZADD/ZRANGE | 完成 |
| [sds.h](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/sds.h) | SDS（简单动态字符串）数据结构的相关声明，用于字符串底层数据结构实现 | 完成 |
| [sds.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/sds.c) | SDS（简单动态字符串）数据结构的具体实现 | 完成 |
| [quicklist.h](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/quicklist.h) | 快速列表数据结构的相关声明，3.2 版本后结合双端链表和压缩列表用于列表键底层数据结构实现，7.0 版本后结合双端列表和紧凑列表用于列表键底层数据结构实现 | 完成 |
| [quicklist.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/quicklist.c) | 快速列表数据结构的具体实现 | 低于一半 |
| [adlist.h](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/adlist.h) | 双端链表数据结构的相关声明，3.2 版本之前作为列表键的底层数据结构实现，也用于其它需要链表的场景，例如保存连接的客户端、慢查询条目等等 | 完成 |
| [adlist.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/adlist.c) |  双端链表数据结构的具体实现 | 完成 |
| [cli_common.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/cli_common.c) | 命令行接口-通用接口的具体实现 | 完成 |
| [intset.h](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/intset.h) |  整数集合数据结构的相关声明，作为集合键的底层实现之一 | 完成 |
| [intset.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/intset.c) | 整数集合数据结构的具体实现 | 完成 |
| [dict.h](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/dict.h) | 字典数据结构的相关声明，用于 redis 中 hashtable（哈希表）的底层实现 | 完成 |
| [zmalloc.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/zmalloc.c) | redis 中内存管理的具体实现 | 低于一半 |
| [ziplist.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/ziplist.c) | ziplist 压缩列表的实现，7.0 版本后被 listpack 紧凑列表替换了 | 低于一半 |
| [sentinel.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/sentinel.c) | sentinel 哨兵机制的具体实现 | 低于一半 |
| [rdb.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/rdb.c) | RDB 持久化功能的具体实现 | 低于一半 |
| [replication.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/replication.c) | 主从复制的具体实现 | 低于一半 |
| [object.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/object.c) | Redis 的对象系统实现 | 低于一半 |
| [util.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/util.c) | Redis 工具函数 | 低于一半 |
| [blocked.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/blocked.c) | Redis 对 BLPOP 和 WAIT 等阻塞操作的通用支持 | 低于一半 |
| [rio.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/rio.c) | 四种 redis IO 类型操作的实现  | 过半 |
| [cluster.h](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/cluster.h) | redis 集群数据结构定义 | 完成 |
| [cluster.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/cluster.c) | redis 集群实现 | 低于一半 |
| [bio.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/bio.c) | redis 后台 IO 线程模型 | 完成 |
| [lazyfree.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/lazyfree.c) | redis 惰性删除功能 | 完成 |
| [ae.h](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/ae.h) | redis 事件循环器结构定义 | 完成 |
| [ae.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/ae.c) | redis 事件循环器功能 | 完成 |
| [multi.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/multi.c) | redis 事务实现  | 完成 |
| [redis-check-rdb.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/redis-check-rdb.c) | Redis RDB 检查工具 | 完成 |
| [redis-check-aof.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/redis-check-aof.c) | Redis AOF 检查工具 | 低于一半 |
| [evict.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/evict.c) | 四种 redis 内存淘汰算法  | 完成 |
| [aof.c](https://github.com/CN-annotation-team/redis7.0-chinese-annotated/blob/7.0-cn-annotated/src/aof.c) | redis AOF 功能  | 过半 |
</p>
尚未有中文注释的文件不会出现在表格中。<br>
更新日期：2022/10/27


## 关于提交 PR 的方法：
### Step1:
首先你需要 fork 本仓库到你自己的 github 仓库，点击右上角的 fork 按钮🎉🎉<br>
### Step2:
使用 git clone 命令将本仓库拷贝到你的本地文件，git clone 地址请点开项目上方的绿色 "code" 按钮查看😀😀<br>
### Step3:
在你的本地对代码进行一番精心修改吧！🍉🍉<br>
### Step4:
修改完后，是时候该上传你的改动到你 fork 来的远程仓库上了。你可以用 git bash，也可以使用 IDE 里的 git 来操作。对于 git 不熟的用户建议使用 IDE，IDE 也更方便写 commit 信息，别忘了写 commit 信息哦！当然我们只是增删改中文注释，如果要直接在 github 上编辑也可以，你可以使用最简单的在线编辑功能（预览文件的时候点击右上角的笔🖊），或者你也可以在你的仓库首页按一下句号键使用 github 提供的在线 vscode 。🤔🤔<br>
### Step5:
上传之后，点进你的仓库主页，会出现一个 "Contribute"，点击它，选择 "Open pull request"，选择好你仓库的分支和你想要在这里合并的分支后，点击 "Create pull request"，之后填写你的 PR 标题和正文内容，就成功提交一个 PR 啦！🍭🍭
### Step6 (optional):
记得检查修改自己的 GitHub Public profile 里的 Name 和 Public email，位置在右上角头像的 Settings 里，因为大多数情况下我们会使用 squash merge 来合并 PRs，此时 squash merge 后产生的新提交作者信息会使用这个 GH 信息（如果你的信息想公开的话）。

## 关于提交 PR 的内容：
### 修改内容：
(1) 给未有中文注释的函数添加中文注释。<br>
(2) 修改或删除意思不明确的，意思有误的，有错别字的中文注释。<br>
(3) 修改不标准的注释格式，修改比较严重的标点错误(中文字用英文逗号、句号、括号、引号实际上不需要修改）。<br>
(4) 给中文注释不足的函数添加注释。
### 注释格式：
(1) 只使用多行注释符号/* */，与 redis 英文注释统一。<br>
(2) 注释里的文字内容要与多行注释符号之间有一个空格。<br>
(3) 同一个多行注释内容中，每一行左端都要加上一个\*，并且对齐。<br>
(4) 英文和中文之间要有一个空格。<br>
(5) 请使用 UTF-8 编码进行注释。<br>
(6) 在遵守上面的格式前提下可以自由发挥<br>
### 注释建议：
(1) 不需要几乎每一句代码都加上注释，比如多次重复出现的同一句代码我们只需要在第一次出现的时候注释，有些意义显而易见的代码并不需要进行注释。如果你需要对一个函数进行大量的解释说明，可以在函数定义的上方空出多行来写注释。<br>
(2) 在代码上一行起的注释，若注释上一行有代码可以多空一行。<br>

### 注释要求：
(1) 若你要对没有中文注释的源码加上注释，请先到仓库的 issue 中查看是否有别人打算负责注释了这块源码，若没有则发起 issue 提出你接下来打算负责的源码部分，防止和别人发生冲突，issue 标题格式为“[新注释] <文件名>”（如：[新注释] aof.c），然后在正文内容中说明你要负责的部分，有很多种说明方式只要能说清楚即可，这里不做限制。请至少完成一个函数的注释工作，不要仅对一个函数的某一小部分进行注释，我们希望每个“勇闯无人区”的“勇士”至少负责一个函数。完成一个函数的注释工作，要在函数定义上方说明该函数的作用，在每个重要的部分和难以理解的部分进行代码注释；对于代码量很少的，且内容没有什么阅读难度的函数，请至少在函数定义上方注释说明该函数的作用。<br>
(2) 本仓库在源码基础上增加和修改中文注释，同时为了注释可以增加空格和空行。若你觉得英文注释存在会影响你的中文注释观感，你可以把英文注释删除，但你最好要在中文注释中保留原英文注释的意思。注意不要修改源代码，不要破坏源代码的结构！如果你真的对源代码有修改的想法，请到 redis 仓库发起 pull request。还有如果你发现了英文注释有误，你想对英文注释进行修改，我们经过审核后可以在这边先进行合并，但同时你也别忘了到 redis 的仓库提一下 pr 告知官方哦！（但是我们的仓库是比较旧的，请先检查官方有没有已经修正了）🥇🥇<br>
(3) 对于 commit 信息，请写清楚你对哪个函数进行了注释的添加或修改。如：“添加 setGenericCommand 函数的注释”，“修正 setGenericCommand 函数注释的格式错误”。<br>
(4) 如果你增加了新的注释，请看一下项目进度的完成度标准，再看看你增加注释后是否达到了一个新的完成度阶段，如果达到了 PR 时请一起修改 README 中的项目进度，谢谢🍭🍭<br>
## FAQ:
### Q: 该项目是定格在 redis7.0 版本了吗？还是会更新呢？
A: 我们希望项目能够持续更新下去，所以会在 redis 发行了一个新版本之后逐步与最新版本的代码合并。🍭🍭
### Q: 我发现了一个注释错误，但是我不会发起 PR。
A: 如果你不会发起 PR，建议上网找个详细的教程，PR 并不难。如果你实在不会...你也可以提 issue 告诉我们哪里有错误来参与修改。
### Q: 我有 redis 相关的问题，可以在这里讨论吗？
A: 如果你有对 redis 源码出 bug 的问题，建议到 redis 仓库去提 issue。如果你有其他的问题（如面试问题，使用问题），命令的相关用法可以到[redis官网](https://redis.io/)查找，网上实在找不到方法也可以在这里讨论，我们特地开设了一个讨论区（上方的 Discussions ）来让大家讨论 redis 相关问题！在讨论区讨论的时候请记得用词要文明礼貌，别人才会愿意和你交流哦！
### Q: 我想注释并提交的代码已经被别人提交了。
A: 这是有可能的，我们不希望你每次都攒了一个很多注释的代码再发起 PR，每次 PR 尽量范围小一些，比如做完两三个函数的注释就发起 PR，减少和别人的冲突。而且PR的内容少一些的话，审查得也会变快哦~如果是对一个尚未有中文注释的源码部分进行注释，请先在 issue 中查看是否有别人打算负责注释了这块源码，若没有则提出你接下来打算负责的源码部分，防止和别人发生冲突。
### Q: 如果我的 PR 和别人的 PR 注释的函数相同怎么办？
A: 我们会选取最好的注释来合并，我们希望把意思最清晰且观感最好的注释展现给大家看。如果你的注释没有被合并，请不要气馁，你可以学习一下被合并的注释对比你的注释优点在哪里，学习之后可以提升你写代码注释的水平哦！🍦🍦
### Q: 我贡献注释能得到什么？
A: 这是一个大家用爱发电的项目，包括我们 "CN-Annotation-Team" 组织的大家都是用爱发电的，金钱方面是没有利益啦...最重要的是以贡献注释的过程中，你阅读和理解源码之后，并能把它讲明白给大家听，这点对你是受益匪浅的，同时贡献注释给他人也许还能成为你阅读并注释源码的动力。<br>
  除此之外，要是你贡献累计达到50行注释以上，我们会邀请你成为 "CN-Annotation-Team" 组织的一员！身为 "CN-Annotation-Team" 组织的一员去完善现有项目的中文注释，或者推动其它热门开源项目的中文注释项目吧！🍭🍭（虽然目前组织还很小也没什么名气，但是这也是个好机会让你可以成为元老级的member啊~）<br>
还有，偷偷告诉你，在你做中文注释的同时，由于你详细阅读了源码，你是有很大机会找出问题给 Redis 提 PR 的哦！<br>

