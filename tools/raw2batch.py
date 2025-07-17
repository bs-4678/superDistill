import os
import json
from tqdm import tqdm
from copy import deepcopy
from superDistill import superDistillation
# 1. 该脚本用于将原始数据转换为批处理数据集。
def raw2batch(raw_data ,batch_data, model_name='deepseek-v3', ratio=[0, 1], sample_num=1e5, single_file_nums=1e4, ans_num=1):
    # "qwen-max-latest", "deepseek-v3"
    template_enhance_qwen3 = """
    # 角色
    你是智能小 Q 产品中的“智能问数”模块，专注于通过自然语言交互帮助用户快速获取数据、验证取数过程，并支持灵活图表交互。支持用户查询具体的数据指标，生成直观的可视化图表等。

    # 对话背景
    prompt（数据表）: {prompt},
    query（用户问题）: {query},
    response（模型回答）: {response}
    raw_infers（模型回答中拆分出来的原始推理过程）: {raw_infers}
    
    # 任务（目的）
    - 仔细阅读以上对话背景，原始答案中的每一步推理过程已经拆分好，存放于raw_infers和后续题目中，你应该参考后续的示例来回答后续题目。

    # 注意事项
    - 扩写的推理需要从 prompt 和 query 中提取出用户问题的关键信息，并思考如何在没有 response 的情况下引导模型出回答 response 中的内容进行推理。
    - 理解与思考中无需涉及任何具体SQL写法或具体实现层面的内容，严禁在推理中使用 QBI-sql 语法或 python 代码，推理内容应为通俗易懂的自然语言描述。其中可以包含对问题本身的模糊语义理解、维度维值理解等。
    - 你不需要关注表中的具体数据，可以假设所有有关数据都存在。
    - 推理内容具备可读性、清晰性、简洁性和准确性，并且可辅助理解SQL生成过程。
    - 每一步推理长度不超过{infer_len}字
    - 由于 QBI-sql 的时间的描述有独特规则，因此在扩写时尽量不要更改时间的描述方式。

    # 示例
    <step_judge>
    该任务由2步 QBI-sql 和0步 python 共2步完成
    </step_judge>
    <step1>
    “最近35周F支付笔数情况排名”扩写为：
    首先分析F客户最近35周的支付行为，筛选时间段并限定性别为F。按商品名称分组统计总支付笔数，降序排列以识别最受F客户欢迎的商品。这为后续M客户的推荐提供参考基准。
    </step1>
    <step2>
    “最近35周各M支付笔数情况排名”扩写为：
    接着分析M客户同期支付数据，同样筛选35周时间段但限定性别为M。按商品名称分组统计支付笔数，升序排列找出M客户购买较少的商品。结合F客户的偏好，推荐这些商品中可能适合M客户但未被充分购买的TOP2商品。
    </step2>

    # 题目
    {question}

    请针对以上任务和示例填写题目中的{{扩写推理内容}}，并将整个题目和填写结果按照示例格式返回{ans_num}个不同的结果，用换行符分隔，不要输出任何解释，并确保可以直接通过对换行符split之后可以直接使用，即不需要任何序号。
    """

    folder_path = raw_data.content['path']  # 替换为你的文件夹路径
    examples_single = []
    examples_multi = []
    sample_examples_single = []
    sample_examples_multi = []
    if ratio[0] > 0:
        examples_single = superDistillation.data_load(folder_path, layers=1, excludes=["195", "多", "知识库"])  # 加载数据，排除包含"195"，"多"的文件
        sample_examples_single = examples_single[::int(len(examples_single)//(sample_num*ratio[0]+1))]
    if ratio[1] > 0:
        examples_multi = superDistillation.data_load(folder_path, layers=1, excludes=["195", "知识库", "example_20_多轮对话", "example_401_多日期字段", "example_121_多步占比"], filters=["多"])  # 加载数据，必须包含"多"的文件
        sample_examples_multi = examples_multi[::int(len(examples_multi)//(sample_num*ratio[1]+1))]
    print(len(examples_single), len(examples_multi))
    sample_examples = sample_examples_single + sample_examples_multi

    batch_sample = [[] for _ in range(int(len(sample_examples)//single_file_nums+1))]

    for i, example in tqdm(enumerate(sample_examples)):
        batch_i = int(i // single_file_nums)
        # new_example = {
            # "custom_id":i,
            # "method":"POST",
        #     "url":"/v1/chat/completions",
        #     "body":{
        #         "model":model_name,
        #         "messages":[
        #             {
        #                 "role":"system",
        #                 "content":"You are a helpful assistant."
        #             },
        #             {
        #                 "role":"user",
        #                 "content":"你好！有什么可以帮助你的吗？"
        #             }
        #         ]
        #     }
        # }
        query = example['query']
        prompt = example['prompt']
        response = example['response']
        sql_nums = example['response'].count("```sql")
        python_nums = example['response'].count("```python")
        raw_infers = superDistillation.super_split(example['response'], ['```sql', '```python', '```'])[::2]
        if any('CREATE' in raw_infers[0] for key in ['CREATE', ]):  # 检查是否包含 CREATE 语句
            print(f"跳过 CREATE 语句的示例：{example['response']}")
            continue  # 跳过包含 CREATE 语句的示例
        infer_len = 100  # 每步推理长度限制
        question = """
            <step_judge>
            该任务由{sql_nums}步 QBI-sql 和{python_nums}步 python 共{step}步完成
            </step_judge>
        """.format(
            sql_nums=sql_nums,
            python_nums=python_nums,
            step=sql_nums+python_nums
        )
        for j, infer in enumerate(raw_infers):
            question += """
            <step{j}>
            “{infer}”扩写为：
            {{扩写推理内容}}
            </step{j}>
            """.format(
                j=j+1,
                infer=infer[:infer_len]
            )
        enhance_prompt = template_enhance_qwen3.format(prompt=prompt, response=response, query=query, step = sql_nums+python_nums, sql_nums = sql_nums, python_nums = python_nums, infer_len = infer_len, raw_infers=raw_infers, question = question, ans_num=ans_num)
        enhance_prompt = enhance_prompt.replace(" ", "").replace("\n\n", "\n")  # 替换换行符为空格
        new_example = {
            "custom_id":i,
            "method":"POST",
            "url":"/v1/chat/completions",
            "body":{
                "model":model_name,
                "messages":[
                    {
                        "role":"system",
                        "content":"You are a helpful assistant."
                    },
                    {
                        "role":"user",
                        "content":enhance_prompt
                    }
                ]
            },
            "_from": example['_from']
        }

        # new_examples.append(deepcopy(new_example))
        batch_sample[batch_i].append(deepcopy(new_example))

    os.makedirs(batch_data.content['path'], exist_ok=True)
    superDistillation.clear_folder(batch_data.content['path'])  # 清空目录

    for batch_i in range(len(batch_sample)):
        with open(batch_data.content['path'] + f"/query_batch_{batch_i}.jsonl", "w", encoding="utf-8") as f:
            for new_example in batch_sample[batch_i]:
                json.dump(new_example, f, ensure_ascii=False)  # 写入 JSON 行
                f.write("\n")  # 换行符，确保每行一个 JSON


    # # 1. 该脚本用于将原始数据转换为批处理数据集。
    # def func1(self, batch_data, model_name='deepseek-v3', ratio=[0, 1], sample_num=1e5, single_file_nums=1e4, ans_num=1):
    #     # "qwen-max-latest", "deepseek-v3"
    #     template_enhance_qwen3 = """
    #     # 角色
    #     你是智能小 Q 产品中的“智能问数”模块，专注于通过自然语言交互帮助用户快速获取数据、验证取数过程，并支持灵活图表交互。支持用户查询具体的数据指标，生成直观的可视化图表等。

    #     # 对话背景
    #     prompt（数据表）: {prompt},
    #     query（用户问题）: {query},
    #     response（模型回答）: {response}
    #     raw_infers（模型回答中拆分出来的原始推理过程）: {raw_infers}
        
    #     # 任务（目的）
    #     - 仔细阅读以上对话背景，原始答案中的每一步推理过程已经拆分好，存放于raw_infers和后续题目中，你应该参考后续的示例来回答后续题目。

    #     # 注意事项
    #     - 扩写的推理需要从 prompt 和 query 中提取出用户问题的关键信息，并思考如何在没有 response 的情况下引导模型出回答 response 中的内容进行推理。
    #     - 理解与思考中无需涉及任何具体SQL写法或具体实现层面的内容，严禁在推理中使用 QBI-sql 语法或 python 代码，推理内容应为通俗易懂的自然语言描述。其中可以包含对问题本身的模糊语义理解、维度维值理解等。
    #     - 你不需要关注表中的具体数据，可以假设所有有关数据都存在。
    #     - 推理内容具备可读性、清晰性、简洁性和准确性，并且可辅助理解SQL生成过程。
    #     - 每一步推理长度不超过{infer_len}字
    #     - 由于 QBI-sql 的时间的描述有独特规则，因此在扩写时尽量不要更改时间的描述方式。

    #     # 示例
    #     <step_judge>
    #     该任务由2步 QBI-sql 和0步 python 共2步完成
    #     </step_judge>
    #     <step1>
    #     “最近35周F支付笔数情况排名”扩写为：
    #     首先分析F客户最近35周的支付行为，筛选时间段并限定性别为F。按商品名称分组统计总支付笔数，降序排列以识别最受F客户欢迎的商品。这为后续M客户的推荐提供参考基准。
    #     </step1>
    #     <step2>
    #     “最近35周各M支付笔数情况排名”扩写为：
    #     接着分析M客户同期支付数据，同样筛选35周时间段但限定性别为M。按商品名称分组统计支付笔数，升序排列找出M客户购买较少的商品。结合F客户的偏好，推荐这些商品中可能适合M客户但未被充分购买的TOP2商品。
    #     </step2>

    #     # 题目
    #     {question}

    #     请针对以上任务和示例填写题目中的{{扩写推理内容}}，并将整个题目和填写结果按照示例格式返回{ans_num}个不同的结果，用换行符分隔，不要输出任何解释，并确保可以直接通过对换行符split之后可以直接使用，即不需要任何序号。
    #     """

    #     folder_path = self.datas['RAW'].content['path']  # 替换为你的文件夹路径
    #     examples_single = []
    #     examples_multi = []
    #     sample_examples_single = []
    #     sample_examples_multi = []
    #     if ratio[0] > 0:
    #         examples_single = self.data_load(folder_path, layers=1, excludes=["195", "多", "知识库"])  # 加载数据，排除包含"195"，"多"的文件
    #         sample_examples_single = examples_single[::int(len(examples_single)//(sample_num*ratio[0]+1))]
    #     if ratio[1] > 0:
    #         examples_multi = self.data_load(folder_path, layers=1, excludes=["195", "知识库", "example_20_多轮对话", "example_401_多日期字段", "example_121_多步占比"], filters=["多"])  # 加载数据，必须包含"多"的文件
    #         sample_examples_multi = examples_multi[::int(len(examples_multi)//(sample_num*ratio[1]+1))]
    #     print(len(examples_single), len(examples_multi))
    #     sample_examples = sample_examples_single + sample_examples_multi

    #     batch_sample = [[] for _ in range(int(len(sample_examples)//single_file_nums+1))]

    #     for i, example in tqdm(enumerate(sample_examples)):
    #         batch_i = int(i // single_file_nums)
    #         # new_example = {
    #             # "custom_id":i,
    #             # "method":"POST",
    #         #     "url":"/v1/chat/completions",
    #         #     "body":{
    #         #         "model":model_name,
    #         #         "messages":[
    #         #             {
    #         #                 "role":"system",
    #         #                 "content":"You are a helpful assistant."
    #         #             },
    #         #             {
    #         #                 "role":"user",
    #         #                 "content":"你好！有什么可以帮助你的吗？"
    #         #             }
    #         #         ]
    #         #     }
    #         # }
    #         query = example['query']
    #         prompt = example['prompt']
    #         response = example['response']
    #         sql_nums = example['response'].count("```sql")
    #         python_nums = example['response'].count("```python")
    #         raw_infers = self.super_split(example['response'], ['```sql', '```python', '```'])[::2]
    #         if any('CREATE' in raw_infers[0] for key in ['CREATE', ]):  # 检查是否包含 CREATE 语句
    #             print(f"跳过 CREATE 语句的示例：{example['response']}")
    #             continue  # 跳过包含 CREATE 语句的示例
    #         infer_len = 100  # 每步推理长度限制
    #         question = """
    #             <step_judge>
    #             该任务由{sql_nums}步 QBI-sql 和{python_nums}步 python 共{step}步完成
    #             </step_judge>
    #         """.format(
    #             sql_nums=sql_nums,
    #             python_nums=python_nums,
    #             step=sql_nums+python_nums
    #         )
    #         for j, infer in enumerate(raw_infers):
    #             question += """
    #             <step{j}>
    #             “{infer}”扩写为：
    #             {{扩写推理内容}}
    #             </step{j}>
    #             """.format(
    #                 j=j+1,
    #                 infer=infer[:infer_len]
    #             )
    #         enhance_prompt = template_enhance_qwen3.format(prompt=prompt, response=response, query=query, step = sql_nums+python_nums, sql_nums = sql_nums, python_nums = python_nums, infer_len = infer_len, raw_infers=raw_infers, question = question, ans_num=ans_num)
    #         enhance_prompt = enhance_prompt.replace(" ", "").replace("\n\n", "\n")  # 替换换行符为空格
    #         new_example = {
    #             "custom_id":i,
    #             "method":"POST",
    #             "url":"/v1/chat/completions",
    #             "body":{
    #                 "model":model_name,
    #                 "messages":[
    #                     {
    #                         "role":"system",
    #                         "content":"You are a helpful assistant."
    #                     },
    #                     {
    #                         "role":"user",
    #                         "content":enhance_prompt
    #                     }
    #                 ]
    #             },
    #             "_from": example['_from']
    #         }

    #         # new_examples.append(deepcopy(new_example))
    #         batch_sample[batch_i].append(deepcopy(new_example))

    #     os.makedirs(batch_data.content['path'], exist_ok=True)
    #     self.clear_folder(batch_data.content['path'])  # 清空目录

    #     for batch_i in range(len(batch_sample)):
    #         with open(batch_data.content['path'] + f"/query_batch_{batch_i}.jsonl", "w", encoding="utf-8") as f:
    #             for new_example in batch_sample[batch_i]:
    #                 json.dump(new_example, f, ensure_ascii=False)  # 写入 JSON 行
    #                 f.write("\n")  # 换行符，确保每行一个 JSON
    #         # with open('/home/wjy488852/data/data_distill/batch_output_1e5.jsonl', 'w') as f:
    #         #     json.dump(new_examples, f, indent=4, ensure_ascii=False)
