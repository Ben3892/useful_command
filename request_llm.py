# 并发请求大模型api
import asyncio
import tqdm.asyncio as tqdma
from openai import AsyncOpenAI
from functools import partial
import os
import json
import argparse
import time
import hashlib

def read_jsonl(file_path):
    ds = []
    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            line = line.strip()
            try:
                if line:
                    ds.append(json.loads(line))
            except:
                continue
    print(f"共读取{len(ds)}条数据")
    print(ds[0].keys())
    return ds

def parse_args():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='数学问题生成和求解脚本')
    
    parser.add_argument('--input_path', type=str, required=True,
                        help='输入文件路径(jsonl格式)')
    parser.add_argument('--file_name', type=str, required=True)
    parser.add_argument('--output_path', type=str, required=True,
                        help='输出文件路径(jsonl格式)')
    parser.add_argument('--prompt_path', type=str, required=True,
                        help='Prompt模板文件路径')
    parser.add_argument('--model_name', type=str, default='gpt-5',
                        help='使用的模型名称(默认: gpt-5)')
    parser.add_argument('--question_column', type=str, default='gemini-2.5-flash出题',
                        help='问题列名(默认: gemini-2.5-flash出题)')
    parser.add_argument('--qps', type=int, default=30,
                        help='每秒请求数(默认: 30)')
    parser.add_argument('--max_retries', type=int, default=2, help="最大重试次数")
    parser.add_argument('--target_column', type=str, default='response')
    parser.add_argument('--stream', type=bool, default=False)
    parser.add_argument('--max_input_length', type=int, default=8192,
                        help='最大输入字符长度(默认: 8192)')
    parser.add_argument('--host', type=str, default=None,
                        help='API端口号(可选)')
    parser.add_argument('--id_column', type=str, default='id',
                        help='唯一标识列名(默认: id)')
    
    return parser.parse_args()

def load_processed_ids(output_path, id_column='id'):
    """从输出文件加载已处理的ID集合"""
    processed_ids = set()
    if os.path.exists(output_path):
        print(f"🔁 加载已处理数据: {output_path}")
        with open(output_path, 'r', encoding='utf-8', errors='ignore') as f:
            for line in f:
                line = line.strip()
                try:
                    if line:
                        item = json.loads(line)
                        if id_column in item:
                            processed_ids.add(item[id_column])
                except:
                    continue
        print(f"✅ 已处理条数: {len(processed_ids)}")
    else:
        print("未发现已处理文件，从头开始")
    return processed_ids

def generate_id_from_text(text):
    """使用SHA256生成文本的唯一ID"""
    if not text:
        return None
    return hashlib.sha256(text.encode('utf-8')).hexdigest()

def filter_unprocessed_data(data, processed_ids, id_column='id', question_column=''):
    """过滤出未处理的数据，并为没有ID的数据生成ID"""
    unprocessed = []
    for item in data:
        # 如果没有id字段，基于question列生成ID
        if id_column not in item or not item[id_column]:
            question_text = item.get(question_column, '')
            generated_id = generate_id_from_text(question_text)
            if generated_id:
                item[id_column] = generated_id
            else:
                # 如果question为空，使用索引作为fallback
                item[id_column] = f"empty_question_{len(unprocessed)}"
        
        # 检查是否已处理
        if item[id_column] not in processed_ids:
            unprocessed.append(item)
    
    print(f"🚀 本轮需处理条数: {len(unprocessed)}")
    return unprocessed

async def get_answer_sample_async(item, aclient, sys_prompt, question_column="", 
                                  model_name="gpt-5", max_retries=2, 
                                  stream=False, max_input_length=8192):
    idx, qa = item
    # qa = sampled_dict[...]，取出一个样本
    '''
    109-118， 数据处理流程，对qa[question_column]（以及sys_prompt）处理获得模型的prompt
    '''
    question = qa.get(question_column, "")
    
    if question == "":
        return idx, qa, ""
    
    question = sys_prompt.replace("{problem}", question)
    
    # 检查输入长度，过滤过长样本，返回空response
    if len(question) > max_input_length:
        return idx, qa, ""

    for attempt in range(1, max_retries + 1):
        try:
            resp = await aclient.chat.completions.create(
                model=model_name,
                messages=[{"role": "user", "content": question}],
                temperature=0.7,
                max_tokens=8192,
                stream=stream,
            )
            if stream:
                text = ""
                async for chunk in resp:
                    if chunk.choices and chunk.choices[0].delta.content:
                        text += chunk.choices[0].delta.content
                return idx, qa, text
            else:
                return idx, qa, resp.choices[0].message.content
        except Exception as e:
            print(f"[{idx}] 第{attempt}次失败: {e}")
            await asyncio.sleep(0.5 * attempt)
    
    return idx, qa, None

async def bound_fetch(sem, item, **kw):
    async with sem:
        return await get_answer_sample_async(item, **kw)

async def get_answer_async(sampled_dict, aclient=None, model_name="gpt-5", 
                          prompt="", max_workers=200, question_column="",
                          target_column="response", max_retries=2, 
                          output_path="", max_input_length=8192):
    sem = asyncio.Semaphore(max_workers)
    
    # 确保输出目录存在
    os.makedirs(os.path.dirname(output_path) if os.path.dirname(output_path) else '.', exist_ok=True)
    
    tasks = [
        bound_fetch(
            sem, item, 
            aclient=aclient, 
            sys_prompt=prompt,
            question_column=question_column,
            model_name=model_name, 
            max_retries=max_retries,
            stream=False,
            max_input_length=max_input_length
        )
        for item in enumerate(sampled_dict)
    ]
    
    completed_count = 0
    progress_interval = 500  # 每N条打印进度
    
    try:
        # 以追加模式打开输出文件
        with open(output_path, 'a', encoding='utf-8') as fout:
            for coro in tqdma.tqdm.as_completed(tasks, total=len(tasks), 
                                                mininterval=10.0, miniters=100):
                idx, qa_item, response = await coro
                
                # 更新结果
                qa_item[target_column] = response
                
                # 立即写入文件
                fout.write(json.dumps(qa_item, ensure_ascii=False) + '\n')
                fout.flush()
                
                completed_count += 1
                
                # 定期打印进度
                if completed_count % progress_interval == 0:
                    print(f"已完成: {completed_count}/{len(sampled_dict)}")
    
    except KeyboardInterrupt:
        print("\n检测到中断，已保存当前进度")
        raise
    
    print(f"✅ 全部完成，结果保存到: {output_path}")
    return sampled_dict

def main(args):
    # 构建完整的输出路径
    output_file = os.path.join(args.output_path, args.file_name)
    
    # 读取原始数据
    original_data = read_jsonl(os.path.join(args.input_path, args.file_name))
    
    # 加载已处理的ID
    processed_ids = load_processed_ids(output_file, args.id_column)
    
    # 过滤出未处理的数据
    unprocessed_data = filter_unprocessed_data(original_data, processed_ids, 
                                               args.id_column, args.question_column)
    
    if len(unprocessed_data) == 0:
        print("🎉 所有数据已处理完成，无需继续")
        return
    
    # 读取prompt模板
    '''
    模型client（url、api相关的）
    '''
    sys_prompt = open(args.prompt_path, 'r', encoding='utf-8').read()
    max_workers = args.qps * 10
    url = f'http://{args.host}'
    
    aclient = AsyncOpenAI(
        base_url=url,
        api_key="",
        max_retries=0,
    )
    
    asyncio.run(
        get_answer_async(
            unprocessed_data,
            aclient=aclient,
            model_name=args.model_name,
            prompt=sys_prompt,
            max_workers=max_workers,
            question_column=args.question_column,
            target_column=args.target_column,
            max_retries=args.max_retries,
            output_path=output_file,
            max_input_length=args.max_input_length
        )
    )

if __name__ == "__main__":
    args = parse_args()
    main(args)