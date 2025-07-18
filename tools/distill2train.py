import os,random,json,traceback,re
import sys
from copy import deepcopy
from tqdm import tqdm
from superDistill import superDistillation

sft_example = {
    "messages": [
        {
            "role": "system",
            "content": ''
        },
        {
            "role": "user",
            "content": ''
        },
        {
            "role": "assistant",
            "content": ''
        }
    ]
}
output_example = {
    "custom_id": "0",
    "method": "POST",
    "url": "/v1/chat/completions",
    "body": {
        "model": "deepseek-v3",
        "messages": [
            {
                "role": "system",
                "content": "You are a helpful assistant."
            }, 
            {
                "role": "user", 
                "content": "\n# nl2sql任务\n     \
                    prompt: ### 当前时间 ###\n2025年09月05日 10:52:20\nTable table_name (客户名称 string comment \"客户名称 枚举值 样例: '朱华强','张东','周华强','陈华','陈鹏华','姚华','唐华','张华','胡磊华','蒋振华'\",\n地区 string comment \"地区 枚举值 样例: '东北','华东','西北','华北','西南','华中','华南'\",\n销售金额 number comment \"销售金额 数值型\",\n商品名称 string comment \"商品名称 枚举值 样例: '洗发水','热水器','电视机','洗衣机','运动鞋','口红','电饭锅','电磁炉','睫毛膏','洗面奶'\",\n配送方式 string comment \"配送方式 枚举值 样例: '干线物流','上门自提','本地配送','城市快递'\",\n销售数量 number comment \"数量 数值型\",\n客户类型 string comment \"客户类型 枚举值 样例: '非会员','会员'\",\n订单编号 string comment \"订单编号 枚举值 样例: 'C104775','C1003611239','C103525','C101940','C1003611213','C10dhfj1123','C100asdf123','C100828','C100361'\",\n订单日期 date comment \"订单日期(year-month-day) 时间\",\n商品类别 string comment \"商品类别 枚举值 样例: '数码','家居清洁','家用电器','母婴用品','服装饰品','食品','电子','美妆护肤'\",\n### CONTEXT ###,\n商品子类别 string comment \"商品子类别 枚举值 样例: '女装','个护清洁','鞋包饰品','男装','美妆','大家电','小家电'\",\n利润金额 number comment \"利润 数值型\",\n省份 string comment \"省份 枚举值 样例: '上海','江苏','广东','浙江','黑龙江','江西','山东','安徽','辽宁','河南'\")\n针对当前待处理表 - DDL, 你是Quick BI的SQL代码助手，请用Quick BI语法的SQL代码回答以下用户问题：\n用户问题：,\n    \
                    query: 最近191天利润金额的加和,\n    \
                    response: SELECT SUM(利润金额)\nFROM table\nWHERE T(订单日期, 'Dp190-Dp0');\n-- QBI 表 = 指标,\n    \n\
                    # 任务\n以上json文件为问题背景、用户问题及模型回答。这是一个nl2sql任务，需分两步用QBI-sql完成。  \n    首先判断这个问题是多步QBI-sql任务，还是单步QBI-sql任务。\n    每步开头需添加100字左右自然语言推理（小白可以看懂，但不要做类比和解释，仅仅是推理过程），要求：  \n    1. 拆解时间范围（如“一季度的最后一个星期”）和聚合逻辑（如“后7天”）；  \n    2. 将QBI语法（T()/日()/分()）与变量引用（${}）的作用通过自然语言描述给阅读者（注意：不要涉及专业语法）；  \n    3. 确保逻辑清晰简洁，辅助理解SQL生成过程。  \n    4. 推理内容应具备可读性、清晰性、简洁性和准确性，最终长度不超过80字。\n    5. 你只需要返回如示例所示的便于小白阅读的推理内容，不需要其他内容。\n\n    示例1：\n    # 这是一个多步QBI-sql任务，分两步完成。\n    ## 步骤1推理：\n    首先明确时间范围，通过特殊函数定位到2025年一季度最后一周的所有日期。然后按天拆分这些日期，对每天的积分过期余额进行求和。将求和结果从小到大排序后，挑出总余额最小的7天。这7天的数据会被保存下来，作为下一步筛选的时间依据。\n    ## 步骤2推理：\n    基于上一步得到的7天，在积分兑换明细中筛选出这7天内的所有记录。接着把每一天按分钟进一步细分，统计每个分钟区间内的历史消耗积分总和。最终得到一张详细的表格，展示这7天里每分钟消耗积分的分布情况。\n\n    示例2：\n    # 这是一个单步QBI-sql任务，一步完成。\n    ## 步骤1推理：\n    首先提取2017年全年数据，按月分组计算销售金额总和。使用日期函数按月聚合销售金额，展示每月销售趋势。结果将展现2017年各月份的销售金额变化情况。\n\n请针对以上任务生成5个回答，用换行符分隔，不要输出任何解释，并确保可以直接通过对换行符split之后可以直接使用，即不需要任何序号。\n"
            }]}}
result_example = {
    "id":"8bfb8c5e-ae44-9e01-91b8-8b559aef2b48",
    "custom_id":"0",
    "response":{
        "status_code":200,
        "request_id":"8bfb8c5e-ae44-9e01-91b8-8b559aef2b48",
        "body":{
            "created":1751382788,
            "usage":{
                "completion_tokens":288,
                "prompt_tokens":997,
                "total_tokens":1285
            },
            "model":"deepseek-v3",
            "id":"chatcmpl-8bfb8c5e-ae44-9e01-91b8-8b559aef2b48",
            "choices":[
                {
                    "finish_reason":"stop",
                    "index":0,
                    "message":{
                        "role":"assistant",
                        "content":"# 这是一个单步QBI-sql任务，一步完成。\n## 步骤1推理：\n首先确定时间范围是从当前日期往前推191天。使用日期函数直接定位这个时间段，对利润金额字段进行求和计算。最终结果将显示最近191天内所有订单的利润总额。\n\n# 这是一个单步QBI-sql任务，一步完成。\n## 步骤1推理：\n用户需要计算过去191天的利润总和。通过日期函数直接筛选出这个时间段内的所有记录，然后用聚合函数计算利润金额的累计值。整个过程只需一步查询即可完成。\n\n# 这是一个单步QBI-sql任务，一步完成。\n## 步骤1推理：\n直接使用日期函数限定订单日期在最近191天内。在这个时间范围内，将所有利润金额相加得到总和。这个查询不需要分步处理，一个简单的求和操作就能满足需求。\n\n# 这是一个单步QBI-sql任务，一步完成。\n## 步骤1推理：\n首先确定时间跨度是从今天往前191天。在这个时间范围内，使用求和函数计算所有利润金额的累计值。整个过程只需要一个简单的聚合查询就能完成。\n\n# 这是一个单步QBI-sql任务，一步完成。\n## 步骤1推理：\n通过日期函数定位到过去191天的数据范围，然后对利润金额字段进行求和运算。这个查询不需要分步处理，直接一步就能得到191天的利润总和。"
                    }
                }
            ],
            "object":"chat.completion"
        }
    },
    "error":None
}
raw_example = {
        "prompt": "### 当前时间 ###\n2024年03月11日 19:18:40\nTable 全平台订单销售_日汇总 (客户婚姻状态 string comment \"婚姻状态 枚举值 样例: '已婚','未婚'\",\n客户id string comment \"客户id 枚举值 样例: '00-167338836583553','00-158775113899535','00-483385221154027','00-164916045676079','00-850104171793937','00-643710693676094','00-320677228299284','00-754212431366960','00-722484460791001','00-702522827303949'\",\n支付金额 number comment \"支付的金额 数值型\",\n商品一级类目 string comment \"商品类目 枚举值 样例: '服饰','图书音像','家用电器','其他','美妆','食品','汽车','运动户外','电子产品'\",\n红包金额 number comment \"使用的红包金额。单位元 数值型\",\n数据日期 date comment \"数据处理的日期 时间\",\n商品名称 string comment \"商品名称 枚举值 样例: '不粘锅具套装','碳纤维登山杖','大众帕萨特','Canon EOS R5','新鲜草莓','骨瓷餐具套装'\",\n客户姓名 string comment \"客户的名字 枚举值 样例: '孙志强','杜鹃','曹颖','宋小宝','马玲'\",\n客户手机号 string comment \"客户的手机号 枚举值 样例: '19100191000','18500185000','17900179000','15200152000','13600136000'\",\n支付笔单价 number comment \"支付的笔单价 数值型\",\n支付笔数 number comment \"支付的笔数 数值型\",\n客户居住状态 string comment \"居住状态 枚举值 样例: '其他','租房','自住'\",\n客户性别 string comment \"男女性别 枚举值 样例: 'M','F'\",\n商品ID string comment \"商品ID 枚举值 样例: '2023_mKRxu07','2023_PFVDuIP','2023_zg9to7N','2023_6P8zzWu','2023_ZTvqPvb','2023_b6GX38R','2023_4gCbg10','2023_74F7mer','2023_l43Y9h4','2023_OiaEw0e'\")\n针对当前待处理表 - DDL, 你是Quick BI的SQL代码助手，请用Quick BI语法的SQL代码回答以下用户问题：\n用户问题：",
        "query": "最近4周有哪些周的红包金额大于等于九百三十七万三千一百四十",
        "sim_querys": [
            "最近4周红包金额大于等于九百三十七万三千一百四十的周",
            "最近4周哪些周的红包金额大于等于九百三十七万三千一百四十",
            "最近4周哪些周的红包金额汇总大于等于九百三十七万三千一百四十",
            "最近4周有哪些周的红包金额大于等于九百三十七万三千一百四十"
        ],
        "response": "SELECT 周(数据日期)\n      ,SUM(红包金额)\nFROM 全平台订单销售_日汇总\nWHERE T(数据日期, 'Wp3-Wp0')\nGROUP BY 周(数据日期)\nHAVING SUM(红包金额) >= 9373140;\n-- QBI 表 = 表格",
        "_from": "example_17_having_where.py.ExampleGenerator.examples_1",
        "split": "train"
}
def distill2train(raw_path=None, batch_path=None, distill_path=None, train_path=None, test_path=None, test_ratio=0.05, raw_ratio=0.):
    sft_raws = []
    if raw_ratio > 0.001:
        # step2:mix
        print("正在采集raw数据。。。")
        raws = superDistillation.data_load(raw_path)
        print('raw_nums', len(raws))

        sample_num = len(sft_results) * raw_ratio
        print("每隔",int(len(raws)//sample_num), "步采样一次")
        raws_sample = raws[::int(len(raws)/sample_num) + random.randint(1, 5)]

        for raw in tqdm(raws_sample):
            new_example = deepcopy(sft_example)
            new_example['messages'][0]['content'] = raw['prompt']
            new_example['messages'][1]['content'] = raw['query']
            new_example['messages'][2]['content'] = raw['response']
            sft_raws.append(new_example)

    print("正在采集batch数据。。。")
    outputs = superDistillation.data_load(batch_path)
    results = sorted(results, key=lambda x: x['custom_id'])
    print('batch_len', len(outputs))
            
    print("正在采集distill数据。。。")
    results = superDistillation.data_load(distill_path)
    results = sorted(results, key=lambda x: x['custom_id'])
    print('distill_len', len(results))

    # step1:sft
    sft_results = []
    sft_results_2 = []
    step2 = 0
    false_count = 0
    lost_num = 0
    for i in tqdm(range(len(results))):
        acc = False
        sft_result = deepcopy(sft_example)
        # output = outputs[i]["body"]["messages"][1]["content"].split("prompt:", "query:", "response:", ",\n    \n# 任务")
        while results[i]['custom_id'] != outputs[i+lost_num]['custom_id']:
            lost_num += 1
            print(f"lost_num: {lost_num}")
        output = re.split(r'prompt: |query: |response: |\n    \n# 任务', outputs[i+lost_num]["body"]["messages"][1]["content"])
        result = results[i]["response"]["body"]["choices"][0]["message"]["content"].split('\n')
        sft_result["messages"][0]["content"] = output[1].replace("请用Quick BI语法的SQL代码回答以下用户问题：", "请用Quick BI语法的SQL代码回答以下用户问题：(请在每一步之前给出你通俗详细的推理描述)")
        sft_result["messages"][1]["content"] = output[2] + " /no_think"
        sft_result["messages"][2]["content"] = "<think>\n\n</think>\n\n"+output[3]
        # 拆分出推理和sql
        responses = output[3].replace('\n    \n', '').replace('\n', '').split("```")
        sft_result["ans"] = output[3]

        sql_count = output[3].count('```sql')
        python_count = output[3].count('```python')
        sft_result["step_nums"] = {'sql_nums':sql_count, 'python_nums':python_count}

        if "###" not in output[1][:5]:
            acc = False
        elif "知识库" in output[1]:
            acc = False
        elif sql_count+python_count == 2:
            while len(result) >= 5:
                if '步骤1' not in result[1] or '步骤2' not in result[3]:
                    result = result[1:]
                else:
                    sft_result["messages"][2]["content"] = output[3].replace(responses[0]+'\n',result[2]+'\n').replace(responses[2]+'\n',result[4]+'\n')
                    acc = True
                    step2 += 1
                    sft_results_2.append(sft_result)
                    break
        elif sql_count+python_count == 0:
            while len(result) >= 3:
                if '步骤1' not in result[1]:
                    result = result[1:]
                else:
                    sft_result["messages"][2]["content"] = result[2] + '\n```sql\n' + output[3] + '\n```'
                    acc = True
                    break
        elif sql_count+python_count == 1:
            while len(result) >= 3:
                if '步骤1' not in result[1]:
                    result = result[1:]
                else:
                    sft_result["messages"][2]["content"] = result[2] + output[3]
                    acc = True
                    break

        if acc == False:
            false_count += 1
            continue
        sft_results.append(sft_result)

    print(len(sft_results),false_count,step2)
            
    sample_examples = sft_raws + sft_results_2
    random.shuffle(sample_examples)

    split_idx = int(len(sample_examples) * (1-test_ratio))
    train_examples = sample_examples[:split_idx]
    test_examples = sample_examples[split_idx:]

    # only 多步
    superDistillation.data_save(train_examples, train_path)
    print(f"已保存多步数据至 {train_path}")

    # only 多步
    superDistillation.data_save(test_examples, test_path)
    print(f"已保存多步数据至 {test_path}")

    # with open('/home/wjy488852/data/data_train/distill5_5e4_sft_twostep.jsonl', "w", encoding="utf-8") as f:
    #     for example in tqdm(sample_examples):
    #         json.dump(example, f, ensure_ascii=False)
    #         f.write("\n")

    n0, n1, n2, n3, n4 = len(sample_examples), len(sft_results), len(sft_raws), len(train_examples) + len(test_examples)
    print(f"Done: 共生成数据{n0}条，其中sft数据{n1}条，raw数据{n2}条，训练数据{n3}条，测试数据{n4}条。")

# def distill2train(raw_path=None, batch_path=None, distill_path=None, train_path=None, test_path=None, test_ratio=0.05, raw_ratio=0.):
#     sft_raws = []
#     if raw_ratio > 0.001:
#         # step2:mix
#         print("正在采集raw数据。。。")
#         raws = superDistillation.data_load(raw_path)
#         print('raw_nums', len(raws))

#         sample_num = len(sft_results) * raw_ratio
#         print("每隔",int(len(raws)//sample_num), "步采样一次")
#         raws_sample = raws[::int(len(raws)/sample_num) + random.randint(1, 5)]

#         for raw in tqdm(raws_sample):
#             new_example = deepcopy(sft_example)
#             new_example['messages'][0]['content'] = raw['prompt']
#             new_example['messages'][1]['content'] = raw['query']
#             new_example['messages'][2]['content'] = raw['response']
#             sft_raws.append(new_example)

#     print("正在采集batch数据。。。")
#     outputs = superDistillation.data_load(batch_path)
#     print('batch_len', len(outputs))
            
#     print("正在采集distill数据。。。")
#     results = superDistillation.data_load(distill_path)
#     print('distill_len', len(results))

#     # step1:sft
#     sft_results = []
#     sft_results_2 = []
#     step2 = 0
#     false_count = 0
#     lost_num = 0
#     for i in tqdm(range(len(results))):
#         acc = False
#         sft_result = deepcopy(sft_example)
#         # output = outputs[i]["body"]["messages"][1]["content"].split("prompt:", "query:", "response:", ",\n    \n# 任务")
#         while results[i]['custom_id'] != outputs[i+lost_num]['custom_id']:
#             lost_num += 1
#             print(f"lost_num: {lost_num}")
#         output = re.split(r'prompt: |query: |response: |\n    \n# 任务', outputs[i+lost_num]["body"]["messages"][1]["content"])
#         result = results[i]["response"]["body"]["choices"][0]["message"]["content"].split('\n')
#         sft_result["messages"][0]["content"] = output[1].replace("请用Quick BI语法的SQL代码回答以下用户问题：", "请用Quick BI语法的SQL代码回答以下用户问题：(请在每一步之前给出你通俗详细的推理描述)")
#         sft_result["messages"][1]["content"] = output[2] + " /no_think"
#         sft_result["messages"][2]["content"] = "<think>\n\n</think>\n\n"+output[3]
#         # 拆分出推理和sql
#         responses = output[3].replace('\n    \n', '').replace('\n', '').split("```")
#         sft_result["ans"] = output[3]

#         sql_count = output[3].count('```sql')
#         python_count = output[3].count('```python')
#         sft_result["step_nums"] = {'sql_nums':sql_count, 'python_nums':python_count}

#         if "###" not in output[1][:5]:
#             acc = False
#         elif "知识库" in output[1]:
#             acc = False
#         elif sql_count+python_count == 2:
#             while len(result) >= 5:
#                 if '步骤1' not in result[1] or '步骤2' not in result[3]:
#                     result = result[1:]
#                 else:
#                     sft_result["messages"][2]["content"] = output[3].replace(responses[0]+'\n',result[2]+'\n').replace(responses[2]+'\n',result[4]+'\n')
#                     acc = True
#                     step2 += 1
#                     sft_results_2.append(sft_result)
#                     break
#         elif sql_count+python_count == 0:
#             while len(result) >= 3:
#                 if '步骤1' not in result[1]:
#                     result = result[1:]
#                 else:
#                     sft_result["messages"][2]["content"] = result[2] + '\n```sql\n' + output[3] + '\n```'
#                     acc = True
#                     break
#         elif sql_count+python_count == 1:
#             while len(result) >= 3:
#                 if '步骤1' not in result[1]:
#                     result = result[1:]
#                 else:
#                     sft_result["messages"][2]["content"] = result[2] + output[3]
#                     acc = True
#                     break

#         if acc == False:
#             false_count += 1
#             continue
#         sft_results.append(sft_result)

#     print(len(sft_results),false_count,step2)
            
#     sample_examples = sft_raws + sft_results_2
#     random.shuffle(sample_examples)

#     split_idx = int(len(sample_examples) * (1-test_ratio))
#     train_examples = sample_examples[:split_idx]
#     test_examples = sample_examples[split_idx:]

#     # only 多步
#     superDistillation.data_save(train_examples, train_path)
#     print(f"已保存多步数据至 {train_path}")

#     # only 多步
#     superDistillation.data_save(test_examples, test_path)
#     print(f"已保存多步数据至 {test_path}")

#     # with open('/home/wjy488852/data/data_train/distill5_5e4_sft_twostep.jsonl', "w", encoding="utf-8") as f:
#     #     for example in tqdm(sample_examples):
#     #         json.dump(example, f, ensure_ascii=False)
#     #         f.write("\n")

#     n0, n1, n2, n3, n4 = len(sample_examples), len(sft_results), len(sft_raws), len(train_examples) + len(test_examples)
#     print(f"Done: 共生成数据{n0}条，其中sft数据{n1}条，raw数据{n2}条，训练数据{n3}条，测试数据{n4}条。")
