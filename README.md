# 架构说明
+ 全文存在SQL中，切片（Section）存在neo4j中
+ Section和Company都以节点形式存储在一个database中
+ SQL中的主要内容：
    + **crawler_main**
+ Neo4j中的主要内容
    + **Article**：文章，包含url链接，pageTime时间，language语言，sqlId在SQL中的US_id（由SQL2Neo4j.py生成）
    + **Section**：切片，目前选择中文512，英文1024，包含content，position在Article中的序号，find_entity标记是否已经被实体抽取过（由`SQL2Neo4j.py`生成）,find_product标记是否已经被product抽取过。
    + **Entity**：在Section原文中的实体，由qwen-3.5-turbo生成（由`SectionNER_RE.py`的`ai_entity_recognition`生成）
    + **EntityObj**：Entity的中文正式实体名，最初被`ai_entity_recognition`生成的时候它只有一个名字。之后会在`ai_relation_extraction_ORG`函数的作用下获得`ALLOWED_ENTITY_TYPES_FOR_ORG={"Company", "Factory", "MiningSite", "Government", "Academic", 'Media', "NGO", "Others"}`之一的Label，所以EntityObj大多是有两个Label的。
    + 由大语言模型生成的实体和关系都会获得一个`qwen=True`的标签<br>
**————上面是实体，下面是关系————**
    + **Section**-**SectionOf**->**Article**：被`SQL2Neo4j.py`生成
    + **Section**-**Mention**->**Entity**：（由`SectionNER_RE.py`的`ai_entity_recognition`生成）
    + **Entity**-**FullNameIs**->**EntityObj**：（由`SectionNER_RE.py`的`ai_entity_recognition`生成）
    + **EntityObj**-**FullNameIs**->**EntityObj**：被`EntityDes.py`生成，是对同义词的二次过滤
    + 其它EntityObj之间可以拥有的关系：`ALLOWED_RELATION_TYPES_FOR_ORG={"SupplyProductTo", "PartnerOf", "OfferFianceService", "WinBidFor", "SubsidiaryOf", "GrantTechTo", "OwnFactory", "OwnMiningSite"}`

+ 其实，OfferFianceService和Media并不是我们想研究的，但是这样可以吸走一些无关的东西以免大模型将其与我们想研究的供应链关系混淆。
+ 除了组织间关系之外，产品和技术的关系也很重要。这部分信息可以单独再访问一次大模型获得，一次要返回的数据太多会导致不准确。
+ 要推断一个公司给另一个公司提供了什么货物，最好先知道这两个公司都生产什么货物。
+ 下面是后续规划：
    + **ProductCategory**：直接从中国《统计用产品分类目录》导入，并且对**full_name**进行了向量化
    <!-- + **ProductModel**：具体的产品型号，让大模型基于原文生成一个对于产品的描述，然后再通过向量匹配建立与**ProductCategory**之间的关系 -->
    + **Product**：Section原文中提到的实体，对其**name**属性进行了向量化，用于和Product.full_name进行匹配。
    + **ManufactoringProcess**：暂时没有进行抽取
    + **Factory**、**MiningSite**：进行了抽取但是暂未实际使用<br>
**————上面是实体，下面是关系————**
    <!-- + **ProductModel**-**BelongToCategory**->**ProductCategory** -->
    + **ProductCategory**-**SubCategoryOf**->**ProductCategory**
    + **Product**-**BelongToCategory**->**ProductCategory**

![开发架构](img\SPLC_v2.png)
+ NER本地化部署
+ neo4j当中既存Company，也存CompanyName（不带后缀），CompanyName附属于Company而存在，不能具有“Supply”这样的关系

# 主要文件
+ `SQL2Neo4j.py`：将SQl中的Article导入Neo4j并以Section形式存储。目前已经成功测试了`SectionRec01`，可能会后续用于识别。但如果这意味着需要对拿进数据库的所有Section进行一次判定，这可能会导致一定的算力浪费。因此，在实际使用中我们可能会考虑优先对Article进行判别，同时考虑其长度问题，优先对于其中得分较高、长度适中的进行Embedding。
+ `refresh_databse_load_time.py`：如果要重新导入，需要先用这个去除SQL中的load_time
+ `QwenEmbedding.py`：通过通义千问v3的512维度模式对Section进行向量化。之后可能会对EntityObj进行相似的操作。目前这个并非必须运行。
+ `NER_RE_Entity.py`：对Entity和EntityObj进行实体识别和关系抽取
    + `ai_entity_recognition`：完成实体抽取、代词还原
    + `ai_relation_extraction_ORG`：完成对给定实体类型判定、关系抽取
+ `EntityDes.py`：给具有连边的实体赋予country、description等属性，并进一步排除不是中文正式全称的情况
+ `NER_RE_Product.py`：对Product进行实体识别和关系抽取
+ `RelationVerify.py`：对SupplyProductTo类型的关系进行判定，确认其方向，并赋予其更加丰富的信息`analysing_process", "status", "product", "amount", "amount_unit", "value", "value_unit"`
+ `ProductCategorize.py`：找到一个产品对应的《统计用产品分类目录》代码
+ `Output.ipynb`：输出gexf文件和图片 

****
下面是暂未发挥作用的代码
+ `Local_NER.py`：用于在本地实现实体关系识别，由于实在太不准确，目前用Qwen代替。


# 库说明
+ `API`库用来处理与各类资源的基础交互功能
+ `text_process`库用来在本地处理自然语言
+ `procedures`用于保存需要调佣API的、更加复杂一些的功能函数
+ `QueryDoc`用来保存一些重要的查询语句（虽然这个其实也可以存在Neo4j里面）
+ `result`当中保存了由output模块导出的数据结果
+ `test`、`weaviate_trails`、`.ipynb`都是用来保存一些目前已经不需要的东西

# 配置说明
+ 原来使用的版本是5.19，现在最新的版本是5.24。社区版下载地址：https://neo4j.com/deployment-center/#community
+ 这里有一个旧版下载的地址：https://we-yun.com/doc/neo4j/
+ 必须使用APOC插件，下载地址：https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases
+ 理论上来说，这个插件只要放在Neo4j的plugin文件夹下就能运行。目前打包了一个`SPLC_Database包含了plugin.zip`是可以解压即用的。
+ 与Neo4j最合适的Java版本是JDK21：https://www.oracle.com/java/technologies/downloads/#jdk21-windows
+ 目前使用的Neo4j路径：E:\Coding\Neo4j\SPLC_Database
+ 数据库备份的正确方法：neo4j-admin database dump neo4j --to-path="E:\Coding\Neo4j\Backup"

# 尝试的历程
+ Weaviate虽然向量检索很快，但向量检索并不是必须项，其实Neo4j也可以做
+ Embedding虽然可能有更好的本地方案，但是确实太慢了，租服务器来做又很难充分利用算力，因此就用通义千问V3了
+ NER本地方案并不慢，但是不准确，不如直接用qwen-3.5-turbo，估计之后本地只会保留一些非深度的小模型
+ 但qwen系列似乎很难处理复杂的json结构生成，因此选择继续使用OpenAI，目前在关系抽取和SupplyVerify的部分用的是gpt-4o（4o相比于3.5虽然并不一定更准确，但似乎总是更戏精一下，愿意给出更深多内容的回答）
+ 关系抽取虽然deepseek也能做得很好，但是它实在是太慢了（30秒左右才能改一个回复）
+ 考虑到SupplyVerify的使用频率不高，可以使用阿里云平台提供的deepseek-r1（qwen蒸馏的deepseek还是差距较大）
+ 本来想使用**ProductModel**来区分产品型号和模糊的产品描述，但是大模型似乎在这一点上还有困难，暂时统一使用**Product**，后续有机会可以从其中将**ProductModel**单独分离出来。