# alimama-B
alimama挑战赛复赛
## 数据范围：

广告数据大小说明：

总文件大小 69G

关键词的数量keywrod(uint64)： ～100万

广告单元的数量adgroup_id(uint64)  ： ～500万

广告计划的数据campaign_id：～100万

广告单元上的关键词数量keywrod(uint64)： ～100


## 服务资源：

服务可运行在最多**3个16G内存16cpu的容器**中

## 数据存储格式：

| keyword(uint64) | adgroup_id(uint64) | price(uint64) | status(int8) |
| --------------- | ------------------ | ------------- | ------------ |
| 2916200016      | 644960096148       | 63885         | 1            |

| timings(int8, 0/1列表，长度24)                  | vector(float列表) | campaign_id(uint64) | item_id(uint64) |
| ----------------------------------------------- | ----------------- | ------------------- | --------------- |
| 0,0,0,0,0,1,0,1,0,0,0,0,0,1,0,0,1,0,1,1,0,1,0,0 | 0.993916,1        | 217245901050        | 646829064714    |

## 请求值：

| keywrod(uint64) | vector(float列表) | hour | topn |
| --------------- | ----------------- | ---- | ---- |
| 2916200016      | 0.351177,0.936309 | 7    | 2    |



## 返回值：

topn个response：

| adgroup_id(uint64)         | prices      |
| -------------------------- | ----------- |
| 644960096148,1710671559561 | 27435,39778 |



## 任务理解：

核心是**广告单元** 我们返回的是广告单元的id

如果一开始的理解没有错误的话 ，一开始的时间是固定的，并且每过一段时间hour字段才会变

- 第一步要处理的就是按照时间对data进行切片

- 第一步为预加载csv

  - 涉及到csv的分块读写

  - 把/data/raw_data.csv中的文件分片读取到内存当中，注意按照总长度进行切分，三个机子或者两个机子分块读取

  - 存储格式为：

  - ```C++
    std::map<uint64_t, uint32_t> keywordID;
    std::map<uint64_t, uint32_t> adgroupID;
    std::vector<std::set<uint32_t>> keywordAdgroupSet;
    std::vector<std::map<uint32_t, pair<float, float>>> keywordAdgroup2vector; 
    std::map<uint32_t, uint32_t> adgroup2price;
    std::map<uint32_t, uint64_t> adgroup2campaign; //后续优化掉或者好像可以直接不用存 不需要 聊天记录里面说了每个单元只会有一个计划
    std::map<uint32_t, uint32_t> adgroup2timings;  //使用2^24次存储 用int就够 
    ```

    

  - 需实现的函数：

    ```C++
    //转换判断类型
    int timings2int(vector<int>& timings, int status);
    int hours2int(int hour);
    bool checkHours(int timing, int hour);
    //csv读取
    void readCsv(string& path);
    
    ```

- 后续为处理用户发送的请求

  - 首先选择keyword的set

  - | keyword(uint64) | vector(float列表) | hour | topn |
    | --------------- | ----------------- | ---- | ---- |
    | 2916200016      | 0.351177,0.936309 | 7    | 2    |

  - 存储格式为：

  - ```c++
    int userKeyword;
    pair<float, float> userVector;
    int hour;
    int topn;
    ```

  - 然后计算每一项的得分

    - 计算方式：

    - 排序分数 = 预估点击率 x 出价（分数越高，排序越靠前）

    - 预估点击率 = 商品向量 和 用户_关键词向量 的余弦距离

    - 计费价格 = 第 i+1 名的排序分数 / 第 i 名的预估点击率（i表示排序名次，例如i=1代表排名第1的广告）

    - ctr = cos(vec(data[i]), vec(user))
    - score = ctr  * prices[i]
    - price = score/ctr

  - 计算和排序注意事项：

    - 排序过程遇到adgroup_id重复，则优先选择排序分数高者，如若同时排序分数相等则取出价低者；若排序过程中排序分数相同，选择出价低者，

    - 出价相同，adgroup_id大的排前面。

    - 浮点运算相关：请计算过程中全部使用float32

    - 点击率预估分计算原始结果加上0.000001f，确保非0

    - 出价结果(uint64)，涉及浮点运算，请按照四舍五入取整

    - 若召回的广告集合少于等于请求的topn时，最后一名的计费价格使用其自身的出价

    - 需要实现的函数：

    - ```c++
      float getCtr(pair<float, float>& userVector, pair<float, float>& itemVector);
      float getScore(float& ctr, int& prices);
      ```

      

  - 构建一个topn的优先队列存储需要的结果响应优先级（排序分数 出价 adgroup_id）

    - 需要实现函数：

    - ```C++
      struct AdGroup {
          float score;
          float price;
          float ctr;
          uint64_t adgroup_id;
          bool operator==(const AdGroup& other) const{
              return adgroup_id == other.adgroup_id;
          }
          bool operator<(const AdGroup& other) const {
              if (score > other.score) {
                  return true;
              } else if (score < other.score) {
                  return false;
              }
      
              if (price < other.price) {
                  return true;
              } else if (price > other.price) {
                  return false;
              }
      
              return adgroup_id > other.adgroup_id;
          }
      };
      
      priority_queue<AdGroup> adGroup;
      ```
      
      
  
  - 对于请求的响应容器 用TLV向队友机器请求相同的返回（这里是否可以在接到请求的时候直接向两个队友机器发送请求，并在自己计算完优先队列之后等待返回在继续操作）
  
    - 需要实现的函数：
  
    - ```
      get()
      request()
      ```
  
  - 将三个优先队列的内容合并为一个优先队列返回







