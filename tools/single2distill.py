import os,random,json,tqdm,traceback
import sys
from openai import OpenAI
from copy import deepcopy
from tqdm import tqdm
from superDistill import superDistillation
import threading
import time
import ast
import queue

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

def single2distill_pro(data_queue, distill_path=None, error_file_path=None, batch_id=0):
    distill_path = os.path.join(distill_path, f"result_single_{batch_id}.jsonl")
    error_file_path = os.path.join(error_file_path, f"error_single_{batch_id}.jsonl")
    
    # 读取原始文件
    examples = data_queue
    # 设置apikey
    client = OpenAI(
        # api_key='sk-ade5e567feba4013a46769eaeb4692b7', 
        # base_url='https://dashscope.aliyuncs.com/compatible-mode/v1' # 阿里云百炼服务的base_url
        api_key='dHVyYXl0Z1B6TExXRUxXSDdXRUE5NmJhWDU2Mkd5ZWZCcmxGWldoTkRhRTpWR0xSOU1vc3VjcXVpUEFrczk5N3ZiME5oTGhPajhjbA',
        base_url="https://aiboost-sit.zacz.cn/api/v1"  # agent_store的base_url
    )
    new_examples = []
    error_examples = []
    count = 0
    while True:
        # 从队列中获取数据，超时则退出
        try:
            example = data_queue.get(timeout=5)
            if example is None:  # 结束信号
                data_queue.task_done()
                break
            data_queue.task_done()  # 标记任务完成
        except queue.Empty:
            # 队列为空，正常退出
            print(f"线程 {batch_id}: 队列为空，线程退出")
            break
        except Exception as e:
            print(f"线程 {batch_id} 处理异常: {e}")
            data_queue.task_done()  # 即使出错也要标记任务完成
            
        if example is None:  # 结束信号
            break
        else:
            print(f"last example num: {examples.qsize()}/4000")
            
        if not example:
            continue
        if 'body' not in example or 'messages' not in example['body'] or len(example['body']['messages']) < 2:
            print(f"Skipping invalid example: {example}")
            continue
        if 'content' not in example['body']['messages'][0] or 'content' not in example['body']['messages'][1]:
            print(f"Skipping example with missing content: {example}")
            continue
        if not example['body']['messages'][0]['content'] or not example['body']['messages'][1]['content']:
            print(f"Skipping example with empty content: {example}")
            continue
        enhance_prompt = example['body']['messages'][0]['content'] + '\n' + example['body']['messages'][1]['content']
        enhance_response = get_response(client, enhance_prompt, "deepseek-v3")
        if enhance_response[0]== "error":
            error_examples.append(deepcopy(example))
            count += 1
            new_examples[-1]['custom_id'] = example['custom_id']
            superDistillation.data_save(error_examples[-1], error_file_path, save_type='a')
            continue
        else:
            new_examples.append(deepcopy(enhance_response[0]))
            new_examples[-1]['custom_id'] = example['custom_id']
            superDistillation.data_save(new_examples[-1], distill_path, save_type='a')
    print(f"Error processing example {count} times.")

def multi_thread_single2distill_pro(batch_path=None, distill_path=None, error_file_path=None, thread_count=2, continue_flag=False):
    batch_all = superDistillation.data_load(data_path=batch_path)       
    # 创建线程安全的队列
    data_queue = queue.Queue()
    

    if continue_flag:
        # 如果继续标志为True，则从distill_path中加载已处理的结果
        if os.path.exists(distill_path):
            last_examples = superDistillation.data_load(data_path=distill_path)
            batch_all = batch_all[len(last_examples):]
        if os.path.exists(error_file_path):
            error_examples = superDistillation.data_load(data_path=error_file_path)

    # 将数据放入队列
    for example in batch_all:
        data_queue.put(example)
        
    superDistillation.data_save(last_examples, os.path.join(distill_path, "result_single_last.jsonl"), save_type='w')
    superDistillation.data_save(error_examples, os.path.join(error_file_path, "error_single_last.jsonl"), save_type='w')
    # 创建线程
    threads = []
    print(f"将使用 {thread_count} 个线程进行处理")
    for batch_id in range(thread_count):
        threads.append(threading.Thread(
            target=single2distill_pro,
            args=(data_queue, distill_path, error_file_path, batch_id)
        ))
    # 启动线程
    for t in threads:
        t.start()

    # 等待线程完成
    for t in threads:
        t.join()

    print(f"共有 {1+thread_count} 个 .jsonl 文件")