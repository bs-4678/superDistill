import os
from pathlib import Path
from openai import OpenAI
from tqdm import tqdm
import time
import requests

base_url='https://dashscope.aliyuncs.com/compatible-mode/v1'
# DASHSCOPE_API_KEY1=sk-7bee41571d7b4ebfb5393d574dd1173f
DASHSCOPE_API_KEY2='sk-ade5e567feba4013a46769eaeb4692b7'
# DASHSCOPE_API_KEY3=sk-da18c7243e8d4fbb84aebb8567668139
# base_url="https://aiboost-sit.zacz.cn/api/v1"
# DASHSCOPE_API_KEY4=dHVyYXl0Z1B6TExXRUxXSDdXRUE5NmJhWDU2Mkd5ZWZCcmxGWldoTkRhRTpWR0xSOU1vc3VjcXVpUEFrczk5N3ZiME5oTGhPajhjbA==

# 初始化客户端
client = OpenAI(
    # 若没有配置环境变量,可用阿里云百炼API Key将下行替换为：api_key="sk-xxx",但不建议在生产环境中直接将API Key硬编码到代码中,以减少API Key泄露风险.
    api_key=DASHSCOPE_API_KEY2,
    base_url=base_url # 阿里云百炼服务的base_url
)
def upload_file(file_path):
    print(f"正在上传包含请求信息的JSONL文件...")
    file_object = client.files.create(file=Path(file_path), purpose="batch")
    print(f"文件上传成功。得到文件ID: {file_object.id}\n")
    return file_object.id

def cancel_batch(batch_id):
    url = f"{base_url}/batches/{batch_id}/cancel"
    headers = {"Authorization": f"Bearer {base_url}"}
    resp = requests.post(url, headers=headers)
    print(f"已取消 batch {batch_id}: 状态码 {resp.status_code}, 反馈 {resp.text}")

def create_batch_job(input_file_id):
    print(f"正在基于文件ID，创建Batch任务...")
    # 请注意:此处endpoint参数值需和输入文件中的url字段保持一致.测试模型(batch-test-model)填写/v1/chat/ds-test,Embedding文本向量模型填写/v1/embeddings,其他模型填写/v1/chat/completions
    batch = client.batches.create(input_file_id=input_file_id, endpoint="/v1/chat/completions", completion_window="24h")
    print(f"Batch任务创建完成。 得到Batch任务ID: {batch.id}\n")
    return batch.id

def check_job_status(batch_id):
    print(f"正在检查Batch任务状态...")
    batch = client.batches.retrieve(batch_id=batch_id)
    print(f"Batch任务状态: {batch.status}\n")
    return batch.status

def get_output_id(batch_id):
    print(f"正在获取Batch任务中执行成功请求的输出文件ID...")
    batch = client.batches.retrieve(batch_id=batch_id)
    print(f"输出文件ID: {batch.output_file_id}\n")
    return batch.output_file_id

def get_error_id(batch_id):
    print(f"正在获取Batch任务中执行错误请求的输出文件ID...")
    batch = client.batches.retrieve(batch_id=batch_id)
    print(f"错误文件ID: {batch.error_file_id}\n")
    return batch.error_file_id

def download_results(output_file_id, output_file_path):
    print(f"正在打印并下载Batch任务的请求成功结果...")
    content = client.files.content(output_file_id)
    # 打印部分内容以供测试
    print(f"打印请求成功结果的前1000个字符内容: {content.text[:1000]}...\n")
    # 追加方式保存结果文件至本地
    with open(output_file_path, "a", indent=4, encoding="utf-8", ensure_ascii=False) as f:
        f.write(content.text)
    print(f"完整的输出结果已追加保存至本地输出文件result.jsonl\n")
    # # 保存结果文件至本地
    # content.write_to_file(output_file_path)
    # print(f"完整的输出结果已保存至本地输出文件result.jsonl\n")

def download_errors(error_file_id, error_file_path):
    print(f"正在打印并下载Batch任务的请求失败信息...")
    content = client.files.content(error_file_id)
    # 打印部分内容以供测试
    print(f"打印请求失败信息的前1000个字符内容: {content.text[:1000]}...\n")
    # 保存错误信息文件至本地
    content.write_to_file(error_file_path)
    print(f"完整的请求失败信息已保存至本地错误文件error.jsonl\n")

def batch_infra(batch_i, input_file_path=None, output_file_path=None, error_file_path=None):
    # 文件路径
    input_file_path = os.path.join(input_file_path, f"query_batch_{batch_i}.jsonl")  # 可替换为您的输入文件路径
    output_file_path = os.path.join(output_file_path, f"result_{batch_i}.jsonl")  # 可替换为您的输出文件路径
    error_file_path = os.path.join(error_file_path, f"error_{batch_i}.jsonl")  # 可替换为您的错误文件路径
    try:
        # Step 1: 上传包含请求信息的JSONL文件,得到输入文件ID,如果您需要输入OSS文件,可将下行替换为：input_file_id = "实际的OSS文件URL或资源标识符"
        input_file_id = upload_file(input_file_path)
        # Step 2: 基于输入文件ID,创建Batch任务
        batch_id = create_batch_job(input_file_id)
        # Step 3: 检查Batch任务状态直到结束
        status = ""
        while status not in ["completed", "failed", "expired", "cancelled"]:
            status = check_job_status(batch_id)
            print(f"等待任务完成...")
            time.sleep(10)  # 等待10秒后再次查询状态
        # 如果任务失败,则打印错误信息并退出
        if status == "failed":
            batch = client.batches.retrieve(batch_id)
            print(f"Batch任务失败。错误信息为:{batch.errors}\n")
            print(f"参见错误码文档: https://help.aliyun.com/zh/model-studio/developer-reference/error-code")
            return
        # Step 4: 下载结果：如果输出文件ID不为空,则打印请求成功结果的前1000个字符内容，并下载完整的请求成功结果到本地输出文件;
        # 如果错误文件ID不为空,则打印请求失败信息的前1000个字符内容,并下载完整的请求失败信息到本地错误文件.
        output_file_id = get_output_id(batch_id)
        if output_file_id:
            download_results(output_file_id, output_file_path)
        error_file_id = get_error_id(batch_id)
        if error_file_id:
            download_errors(error_file_id, error_file_path)
            print(f"参见错误码文档: https://help.aliyun.com/zh/model-studio/developer-reference/error-code")
    except Exception as e:
        cancel_batch(batch_id)
        print(f"An error occurred: {e}")
        print(f"参见错误码文档: https://help.aliyun.com/zh/model-studio/developer-reference/error-code")

def batch_list():
    batches = client.batches.list(after="batch_xxx", limit=2,extra_query={'ds_name':'任务名称','input_file_ids':'file-batch-xxx,file-batch-xxx','status':'completed,expired','create_after':'20250304000000','create_before':'20250306123000'})
    print(batches)
    print(batches.list())

def batch2distill(batch_path=None, distill_path=None, error_file_path=None, start=0, end=10):
    error_count = 0
    for i in tqdm(range(start, end)):
        try:
            print(f"正在处理第 {i} 个 .jsonl 文件")
            batch_infra(batch_i=i, input_file_path=batch_path, output_file_path=distill_path, error_file_path=error_file_path)
        except Exception as e:
            print(f"An error occurred: {e}")
            error_count += 1
            continue
    print(f"{start}-{end}处理完成，共有 {error_count} 个错误")

import threading
import time

def multi_thread_batch2distill(batch_path=None, distill_path=None, error_file_path=None, thread_count=2):
    count = sum(1 for fname in os.listdir(batch_path) if fname.endswith('.jsonl'))
    print(f"共有 {count} 个 .jsonl 文件")
    # 创建线程
    threads = []
    if thread_count > count:
        thread_count = count
    print(f"将使用 {thread_count} 个线程进行处理")
    per_thread_count = round(count / thread_count)
    start_idx = 0
    end_idx = per_thread_count
    for i in range(thread_count):
        threads.append(threading.Thread(
            target=batch2distill, 
            args=(batch_path, distill_path, error_file_path, start_idx, end_idx)
        ))
        start_idx += per_thread_count
        end_idx += per_thread_count
        if i == thread_count - 1:
            end_idx = count  # 确保最后一个线程处理到文件末尾

    # 启动线程
    for t in threads:
        t.start()

    # 等待线程完成
    for t in threads:
        t.join()

    print(f"共有 {count} 个 .jsonl 文件")