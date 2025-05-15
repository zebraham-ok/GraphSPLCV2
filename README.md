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
    所有的关系中，都会有reference_section, original_content, url来指明其出处
    + **Section**-**SectionOf**->**Article**：被`SQL2Neo4j.py`生成
    + **Section**-**Mention**->**Entity**：由`SectionNER_RE.py`的`ai_entity_recognition`生成
    + **Entity**-**FullNameIs**->**EntityObj**：由`SectionNER_RE.py`的`ai_entity_recognition`生成
    + **EntityObj**-**FullNameIs**->**EntityObj**：被`EntityDesGoogle.py`中的`handle_same_entity_relation`生成，是对同义词的二次过滤
    + 其它EntityObj之间可以拥有的关系：`ALLOWED_RELATION_TYPES_FOR_ORG = {"SupplyProductTo", "PartnerOf", "OfferFianceService", "WinBidFor", "SubsidiaryOf", "GrantTechTo", "OwnFactory", "OwnMiningSite"}`

+ 其实，OfferFianceService和Media并不是我们想研究的，但是这样可以吸走一些无关的东西以免大模型将其与我们想研究的供应链关系混淆。
+ 除了组织间关系之外，产品和技术的关系也很重要。这部分信息可以单独再访问一次大模型获得，一次要返回的数据太多会导致不准确。
+ 要推断一个公司给另一个公司提供了什么货物，最好先知道这两个公司都生产什么货物。
+ 下面是后续规划：
    + **ProductCategory**：直接从中国《统计用产品分类目录》导入，并且对**full_name**进行了向量化
    <!-- + **ProductModel**：具体的产品型号，让大模型基于原文生成一个对于产品的描述，然后再通过向量匹配建立与**ProductCategory**之间的关系 -->
    + **Product**：Section原文中提到的实体，对其**name**属性进行了向量化，用于和ProductCategory进行匹配，但从目前的效果来看，匹配《统计用产品分类目录》的分析价值有限，后续可能以自建分类体系为主。
    + **ManufactoringProcess**：暂时没有进行抽取
    + **Factory**、**MiningSite**：进行了抽取但是暂未实际使用<br>
**————上面是实体，下面是关系————**
    <!-- + **ProductModel**-**BelongToCategory**->**ProductCategory** -->
    + **ProductCategory**-**SubCategoryOf**->**ProductCategory**
    + **Product**-**BelongToCategory**->**ProductCategory**

![开发架构](img\SPLC_v2.png)
+ 大模型主要使用阿里云百炼平台，仅保留少量的云雾API作为备用
+ 由于社区版Neo4j不允许多数据库，目前将所有节点混存在neo4j库当中

# 主要文件夹
+ `API`库用来处理与各类资源的基础交互功能
    + `ai_ask.py`用来处理与大模型的交互，主要包含了`ask_qwen`,`ask_gpt`和`get_qwen_embedding`
    + `neo4j_SPLC.py`处理与Neo4j的交互，在早先的版本中曾错误写作`neo4j_SLPC`，目前已经更正
    + `liang_google_search.py`用亮数据API获取Google检索页面的信息，这个主要是做企业信息介绍用的
    + `Mongo_SPLC.py`用于处理与MongoDB的交互，存储亮数据的Google检索页面，以免反复获取一样的信息费钱
+ `auxiliary`库用来存储一些辅助性的功能，目前包括：
    + `line_count.py`用来查看目前一共写了多少行代码
    + `show.py`用来绘制供应链上下游两级的图片（一般是给老板看用的）
+ `info`存储了分类信息
    + `bloomberge_revise.json`是彭博的分类法，目前没使用
    + `ic_category.json`是对于芯片产品的分类
    + `ic_fab_category.json`是对于企业所属行业的分类
+ `main`当中是主要的代码模块
    + `EntityDesGoogle.py`：用于给公司节点进行有针对性的信息增强（主要依据亮数据和大模型检索能力）
        + `get_ai_enriched_info`：给具有连边的实体赋予country、stock_code_list和stock_ticker_list等属性，并进一步排除不是中文正式全称的情况
        + `get_ai_enriched_category`：用于给公司属于什么行业、主要生产什么产品进行归类，提供description、industry_1st、industry_2nd等属性信息
        + `ai_chip_type_check`：对于芯片直接相关的企业，询问大模型它的产品构成
    + `NER_RE_Entity.py`：对Entity和EntityObj进行实体识别和关系抽取
        + `ai_entity_recognition`：完成实体抽取、代词还原
        + `ai_relation_extraction_ORG`：完成对给定实体类型判定、关系抽取
    + `NER_RE_Product.py`：对Product进行实体识别和关系抽取
    + `ProductCateRec.py`：找到一个产品对应的《统计用产品分类目录》代码（暂时停用）
    + `QwenEmbedding.py`：通过通义千问v3的512维度模式对Section进行向量化。之后可能会对EntityObj进行相似的操作。目前这个并非必须运行。
    + `SQL2Neo4j.py`：将SQl中的Article导入Neo4j并以Section形式存储。目前已经成功测试了`SectionRec01`，可能会后续用于识别。但如果这意味着需要对拿进数据库的所有Section进行一次判定，这可能会导致一定的算力浪费。因此，在实际使用中我们可能会考虑优先对Article进行判别，同时考虑其长度问题，优先对于其中得分较高、长度适中的进行Embedding。
    + `SupplyVerify.py`：对SupplyProductTo类型的关系进行判定，确认其方向，并赋予其更加丰富的信息`analysing_process", "status", "product", "amount", "amount_unit", "value", "value_unit"`
+ `model`中保存了需要被调用的pytorch模型，目前只有一个`des_w2_rc70.pth`是实际被使用的
+ `text_process`库用来在本地处理自然语言
    + `chunks.py`用来执行文本切片
    + `find_json.py`用来寻找大模型返回的字符串中json在哪里
    + `file_process.py`主要包含了`sanitize_filename`，`save_json`和`file_freshness`三个功能
+ `procedures`用于保存需要调用多个API的、更加复杂一些的功能函数
    + `ArticleDiscriminate.py`被`assign_score2sql.py`调用，用来执行文章鉴别，判断其是否值得被大模型处理
    + `output_backup.py`用来支持`output_years.py`进行数据导出
+ `result`当中保存了由output模块导出的数据结果
+ `test`、`Previous`都是用来保存一些目前已经不需要的东西

# MongoDB的配置
目前一共使用了三个MongoDB模块，其中1个在爬虫部分，2个在处理部分。数据库名称都叫splc
+ 爬虫部分：使用了google_search_result对亮数据返回的检索信息进行存储（转换为bing格式存储）
+ 处理部分：
    + article_embedding：对于文章及其摘要的嵌入进行存储，以sqlId作为索引
    + google_company：在生成公司摘要过程中进行的网页检索（以google原格式存储）

****
下面是暂未发挥作用的代码
+ `Local_NER.py`：用于在本地实现实体关系识别，由于实在太不准确，目前用Qwen代替。
+ `refresh_databse_load_time.py`：如果要重新导入，需要先用这个去除SQL中的load_time（目前这个几乎已经没有用了）
+ `reason_for_stop.py`：这是一个试验性的代码，用于查看SupplyProductTo.status属性为Stopped的具体原因

# 配置说明
+ 原来使用的版本是5.19，现在最新的版本是5.24，社区版下载地址：https://neo4j.com/deployment-center/#community
+ 这里有一个旧版下载的地址：https://we-yun.com/doc/neo4j/
+ 必须使用APOC插件，下载地址：
    + http://doc.we-yun.com:1008/doc/neo4j-apoc/5.4.0/
    + https://github.com/neo4j-contrib/neo4j-apoc-procedures/releases
+ 理论上来说，这个插件只要放在Neo4j的plugin文件夹下就能运行。目前打包了一个`SPLC_Database包含了plugin.zip`是可以解压即用的。
+ 与Neo4j最合适的Java版本是JDK21：https://www.oracle.com/java/technologies/downloads/#jdk21-windows
+ 目前使用的Neo4j路径：E:\Coding\Neo4j\SPLC_Database
+ 数据库备份的正确方法：neo4j-admin database dump neo4j --to-path="E:\Coding\Neo4j\Backup"

# 数据合规说明
+ 在爬虫层面，我们没有刻意绕过网站的反爬系统，基本上不构成问题
+ 在数据出境层面，可能构成问题，按照《数据出境安全评估申报指南（第二版）》，可能需要在网信办进行申报：https://www.cac.gov.cn/2024-03/22/c_1712783131692707.htm

# 尝试的历程
+ Weaviate虽然向量检索很快，但向量检索并不是必须项，其实Neo4j也可以做
+ Embedding虽然可能有更好的本地方案，但是确实太慢了，租服务器来做又很难充分利用算力，因此就用通义千问V3了
+ NER本地方案并不慢，但是不准确，不如直接用qwen-3.5-turbo，估计之后本地只会保留一些非深度的小模型
+ 但qwen系列似乎很难处理复杂的json结构生成，因此选择继续使用OpenAI，目前在关系抽取和SupplyVerify的部分用的是gpt-4o（4o相比于3.5虽然并不一定更准确，但似乎总是更戏精一下，愿意给出更深多内容的回答）
+ 关系抽取虽然deepseek也能做得很好，但是它实在是太慢了（30秒左右才能改一个回复），阿里云百炼平台上的DeepSeek-R1-Distill-Llama-8B其实也不错，但是比gpt-4o的幻觉会多一些，多语种效果可能也不够好。llama-4-maverick-17b-128e-instruct还要更好一点，但是目前qwen平台上对这个是限流的。
+ 考虑到SupplyVerify的使用频率不高，可以使用阿里云平台提供的deepseek-r1（qwen蒸馏的deepseek还是差距较大）
+ 本来想使用**ProductModel**来区分产品型号和模糊的产品描述，但是大模型似乎在这一点上还有困难，暂时统一使用**Product**，后续有机会可以从其中将**ProductModel**单独分离出来。
+ 目前使用的企业分类法虽然仍然存在一定的问题（比如三级产品分类很难概括一个公司的整体情况，产品分类与行业分类相重叠等问题），但有时候三级分类的存在其实是为了让大模型更好地理解二级分类，在实际处理数据的时候我们仅使用二级分类就可以了。
+ 在尝试与FactSet进行对齐的过程中，发现FactSet中其实同一个公司在不同时期的信息是不一样的，因此造成了可能会重复创建公司节点（这些重复创建的节点一般company_subsidiary或者company_keyword与主节点是不一样的，具有的连边数量远少于主节点。因此准确地识别主体，应当采取name、ticker、company_subsidiary和company_keyword结合的方法。这些都一样的节点理论上应该将其合并了。
+ 修改了neo4j笛卡尔积的低效操作后，代码整体的效率有大幅提升，但是受限于阿里云的速率限制，速度仍然有限。