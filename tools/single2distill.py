import os,random,json,tqdm,traceback
import sys
from openai import OpenAI
from copy import deepcopy
from tqdm import tqdm
from superDistill import superDistillation
import threading
import time
import ast

def get_response(client: OpenAI, content: str, model: str = "deepseek-v3", max_retries=20):
    for attempt in range(max_retries):
        try:
            completion = client.chat.completions.create(
                model=model,
                messages=[{'role': 'user', 'content': content}],
                stream=False,
                extra_body={"enable_thinking": False}
            )
            # 返回 dict
            if hasattr(completion, "model_dump"):
                return [completion.model_dump()]
            elif hasattr(completion, "to_dict"):
                return [completion.to_dict()]
            else:
                return [completion]
        except Exception as e:
            # print(f"Error occurred: {e}")
            # traceback.print_exc()
            # 检查是否是 500 错误，决定是否重试
            if "InternalServerError" in str(type(e)) or "500" in str(e):
                # print(f"第 {attempt+1} 次重试...")
                time.sleep(2)  # 等待2秒后重试
                continue
            # 提取字典部分
            dict_str = str(e).split(" - ", 1)[-1]
            try:
                error_dict = ast.literal_eval(dict_str)
                return ["error", error_dict]
            except:
                return ["error", str(e)]
    return ["error", "Max retries exceeded"]

def single2distill(batch_path=None, distill_path=None, error_file_path=None, batch_id=0, continue_flag=False):
    batch_path = os.path.join(batch_path, f"query_batch_{batch_id}.jsonl")
    distill_path = os.path.join(distill_path, f"result_single_{batch_id}.jsonl")
    error_file_path = os.path.join(error_file_path, f"error_single_{batch_id}.jsonl")
    
    # 读取原始文件
    examples = superDistillation.data_load(data_path=batch_path)
    # 设置apikey
    client = OpenAI(
        # api_key='sk-ade5e567feba4013a46769eaeb4692b7', 
        # base_url='https://dashscope.aliyuncs.com/compatible-mode/v1' # 阿里云百炼服务的base_url
        api_key='dHVyYXl0Z1B6TExXRUxXSDdXRUE5NmJhWDU2Mkd5ZWZCcmxGWldoTkRhRTpWR0xSOU1vc3VjcXVpUEFrczk5N3ZiME5oTGhPajhjbA',
        base_url="https://aiboost-sit.zacz.cn/api/v1"  # agent_store的base_url
    )
    new_examples = []
    error_examples = []
    if continue_flag:
        # 如果继续标志为True，则从distill_path中加载已处理的结果
        if os.path.exists(distill_path):
            new_examples = superDistillation.data_load(data_path=distill_path, mode='jsonl')
        # if os.path.exists(error_file_path):
            # error_examples = superDistillation.data_load(data_path=error_file_path, mode='jsonl')
    count = 0
    for example in tqdm(examples[len(new_examples):], desc=f"Processing batch {batch_id}"):
        enhance_prompt = example['body']['messages'][0]['content'] + '\n' + example['body']['messages'][1]['content']
        enhance_response = get_response(client, enhance_prompt, "deepseek-v3")
        if enhance_response[0]== "error":
            error_examples.append(deepcopy(example))
            count += 1
            # superDistillation.data_save(error_examples, error_file_path)
            superDistillation.data_save(error_examples[-1], error_file_path, save_type='a')
            # with open(error_file_path, 'w') as f:
            #     json.dump(error_examples, f, indent=4, ensure_ascii=False)
            continue
        else:
            new_examples.append(deepcopy(enhance_response[0]))
            # superDistillation.data_save(new_examples, distill_path)
            superDistillation.data_save(new_examples[-1], distill_path, save_type='a')
            # with open(distill_path, 'w') as f:
            #     json.dump(new_examples, f, indent=4, ensure_ascii=False)
    print(f"Error processing example {count} times.")


def multi_thread_single2distill(batch_path=None, distill_path=None, error_file_path=None, thread_count=2, continue_flag=False):
    count = sum(1 for fname in os.listdir(batch_path) if fname.endswith('.jsonl'))
    print(f"共有 {count} 个 .jsonl 文件")
    # 创建线程
    threads = []
    thread_count = count
    print(f"将使用 {thread_count} 个线程进行处理")
    for i in range(thread_count):
        threads.append(threading.Thread(
            target=single2distill,
            args=(batch_path, distill_path, error_file_path, i, continue_flag)
        ))
    # 启动线程
    for t in threads:
        t.start()

    # 等待线程完成
    for t in threads:
        t.join()

    print(f"共有 {count} 个 .jsonl 文件")