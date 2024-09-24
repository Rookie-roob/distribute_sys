* 首先一个很重要的点是raft层是可以把同样的命令commit多次的！！！但是在server层只能apply一次！！！（考虑如果server层多次apply相同命令，对于append连续两次的情况）
* 为什么在应用到server的state machine之前要进行term的判断？
* 防止把之前term的也给提交了（之前term的时候可能这个对应的server不是leader）。
* 那么对于之前term大多数server都已经落到log上，而且已经commit，在即将从raft层到server的state machine时，新的term开始了呢？
* 导致请求超时多发一次请求就可以了，相比于上面的term考虑，这种情况通过重发请求是可以解决的。
* commit过后的日志条目肯定会一直在之后的leader中，而且leader只会append日志（除了快照），在leader中commit的顺序就是我们操作的顺序，所以我们的notifychan以commitIndex作为字典的key！
