* 首先一个很重要的点是raft层是可以把同样的命令commit多次的！！！但是在server层只能apply一次！！！（考虑如果server层多次apply相同命令，对于append连续两次的情况）
* 为什么在应用到server的state machine之前要进行term的判断？
* 新当选的 Leader 的 Log 中还存在已提交未应用的 command，若不进行判断就会尝试向并不存在的 RPC Handler 转发数据并造成阻塞。这种情况解决起来也比较简单，不属于当前 term 的 command 无需转发，直接给状态机应用就可以了。
* 那么对于之前term大多数server都已经落到log上，而且已经commit，在即将从raft层到server的state machine时，新的term开始了呢？
* 新的leader肯定是会包含这个commit后但还没apply的log的，因此会在新leader进行apply，对于client，会超时重发，最后在新的leader那儿得到apply的结果。
* commit过后的日志条目肯定会一直在之后的leader中，而且leader只会append日志（除了快照），在leader中commit的顺序就是我们操作的顺序，所以我们的notifychan以commitIndex作为字典的key！
