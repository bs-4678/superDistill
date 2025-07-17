# 1. data:data0-->datan{path,from,step}
# 2. model:teacher>>student>>baseline
import os, sys, json, random, tqdm, glob
from copy import deepcopy
import subprocess
from queue import Queue
from tqdm import tqdm
import shutil

class nodeList():
    def __init__(self, nodes=[]):
        self.nodes = nodes
    
    def add_node(self, node):
        """
        添加节点到节点列表。
        """
        if node not in self.nodes:
            self.nodes.append(node)

class superNode():
    def __init__(self, node_list=None, sons=None, fathers=None, ancestors=None, content=None):
        self.sons = sons if sons is not None else []  # 子节点列表
        self.fathers = fathers if fathers is not None else []  # 父节点列表
        self.ancestors = ancestors if ancestors is not None else []
        self.content = content if content is not None else {}  # 节点内容
        self.node_list = node_list
        if self.fathers == []:  # 如果没有父节点，则当前节点为根节点
            self.fathers = [self]  # 根节点的父节点为自己
            self.ancestors = [self]  # 根节点没有祖先节点
            self.node_list = nodeList([self]) if self.node_list is None else self.node_list  # 如果没有节点列表，则创建一个新的节点列表
        elif self.node_list is None:  # 如果没有节点列表，则创建一个新的节点列表
            self.node_list = fathers[0].node_list
            self.node_list.add_node(self)
        for father in self.fathers:
            father.sons.append(self)  # 将当前节点添加到父节点的子节点列表中
            self.ancestors += father.ancestors  # 祖先节点列表
        self.ancestors = list(set(self.ancestors))  # 去重祖先节点列表
    
    def get_all_content(self, index=None):
        """
        获取节点的所有内容，包括子节点、父节点、祖先节点和当前节点内容。
        """
        all_content = {
            "sons": self.sons,
            "fathers": self.fathers,
            "ancestors": self.ancestors,
            "content": self.content,
        }
        return all_content

    def save_content(self, path):
        """
        保存节点内容到指定路径。
        """
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(self.get_all_content(), f, ensure_ascii=False, indent=4)

    def save_all(self, path):
        """
        保存节点的整个族谱为 json，从祖先节点开始。同时在json最后指明当前节点。
        """
        genealogy = []
        for node in self.node_list.nodes:
            genealogy.append({
                "sons": [self.node_list.nodes.index(son) for son in node.sons],
                "fathers": [self.node_list.nodes.index(father) for father in node.fathers],
                "ancestors": [self.node_list.nodes.index(ancestor) for ancestor in node.ancestors],
                "content": node.content
            })
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(genealogy, f, ensure_ascii=False, indent=4)

    def load_all(self, path):
        """
        恢复节点的整个族谱，从祖先节点开始。返回全部节点，当前节点在第一位。
        """
        genealogy = []
        with open(path, 'r', encoding='utf-8') as f:
            nodes = nodeList(json.load(f))  # 从文件中加载族谱数据
        for node_content in nodes.nodes:
            node = superNode(
                sons=[],
                fathers=[],
                ancestors=[],
                node_list=genealogy,
                content=node_content['content']
            )
            node.sons=node_content['sons']
            node.fathers=node_content['fathers']
            node.ancestors=node_content['ancestors']
            genealogy.append(node)  # 创建节点对象并添加到族谱列表中

        for node in genealogy:            # 设置子节点
            node.sons = [genealogy[j] for j in node.sons]
            # 设置父节点
            fathers = []
            for j in node.fathers:
                if type(j) == int:  # 如果是索引
                    fathers.append(genealogy[j])
                else:  # 如果是节点对象
                    fathers.append(j)
            node.fathers = fathers
            # 设置祖先节点
            ancestors = []
            for j in node.ancestors:
                if type(j) == int:  # 如果是索引
                    ancestors.append(genealogy[j])
                else:  # 如果是节点对象
                    ancestors.append(j)
            node.ancestors = ancestors
            node.node_list = nodeList(genealogy)  # 设置节点列表
        return genealogy[0]
    
    def show_content_only(self):
        """
        打印当前节点的内容。
        """
        print(f"Node Content: {self.content}")

    def show(self, node=None):
        """
        打印当前节点的内容。
        """
        if node is None:
            node = self
        print(f"Node Content: {node.content}")
        print(f"Sons: ")
        for son in node.sons:
            son.show_content_only()
        print(f"Fathers: ")
        for father in node.fathers:
            father.show_content_only()
        print(f"Ancestors: ")
        for ancestor in node.ancestors:
            ancestor.show_content_only()
        print(f"Node ID: {self.node_list.nodes.index(node)}")
        # print(f"Sons: {son.show_content_only()}" for son in node.sons)
        # print(f"Fathers: {father.show_content_only()}" for father in node.fathers)
        # print(f"Ancestors: {ancestor.show_content_only()}" for ancestor in node.ancestors)
        # print(f"Node ID: {self.node_list.nodes.index(node)}")
    
    def show_all(self):
        """
        打印当前节点的所有内容，包括子节点、父节点、祖先节点和当前节点内容。
        """
        for node in self.node_list.nodes:
            self.show(node)
            print("-" * 40)

    def delete_node(self):
        """
        删除当前节点，并从父节点和子节点中移除。
        不推荐使用，因为会破坏节点之间的关系。导致 node_list 碎片化
        """
        for father in self.fathers:
            father.sons.remove(self)
        for son in self.sons:
            son.fathers.remove(self)
        self.node_list.nodes.remove(self)  # 从节点列表中移除当前节点
        if self.ancestors[0] == self:
            for node in self.node_list.nodes:
                node.ancestors.remove(self)  # 从所有节点的祖先列表中移除当前节点
                if node.fathers != []:  # 如果有父节点
                    node.ancestors += node.fathers[0].ancestors  # 将父节点添加到祖先列表中
                else:
                    node.fathers.append(node)  # 如果没有父节点，则将当前节点添加到父节点列表中
                    node.ancestors.append(node)
        return self

class superData(superNode):
    def __init__(self, path='No path', task='task0', stage='defult', description='defult', node_list=None, sons=None, fathers=None, ancestors=None, content=None):
        super().__init__(node_list=node_list, sons=sons, fathers=fathers, ancestors=ancestors, content=content)
        if 'task' not in self.content:
            self.content['task'] = task  # 任务列表
        if 'stage' not in self.content:
            self.content['stage'] = stage # 任务阶段
        if 'description' not in self.content:
            self.content['description'] = description # 任务描述
        if 'name' not in self.content:
            self.content['name'] = f"data_{self.content['task']}_{self.content['stage']}_{self.content['description']}" # 数据集名称，自动生成路径时需要使用
        if 'path' not in self.content:
            self.content['path'] = path # 数据集路径
        if '*' in self.content['path']: # 如果以*结尾，则为自动生成路径
            self.content['path'] = self.content['path'].replace('*', self.content['name']) # # 例如：./batch/* -> ./batch/batch_data_0, ./batch/*.json -> ./batch/batch_data_0.json
        if 'No path' not in self.content['path']:
            os.makedirs(self.content['path'], exist_ok=True)  # 创建目录，如果目录已存在则不报错

class superModel(superNode):
    def __init__(self, path='No path', task='task0', stage='defult', description='defult', node_list=None, sons=None, fathers=None, ancestors=None, content=None):
        super().__init__(node_list=node_list, sons=sons, fathers=fathers, ancestors=ancestors, content=content)
        if 'task' not in self.content:
            self.content['task'] = task  # 任务列表
        if 'stage' not in self.content:
            self.content['stage'] = stage # 任务阶段
        if 'description' not in self.content:
            self.content['description'] = description # 任务描述
        if 'name' not in self.content:
            self.content['name'] = f"model_{self.content['task']}_{self.content['stage']}_{self.content['description']}" # 数据集名称，自动生成路径时需要使用
        if 'path' not in self.content:
            self.content['path'] = path # 数据集路径
        if '*' in self.content['path']: # 如果以*结尾，则为自动生成路径
            self.content['path'] = self.content['path'].replace('*', self.content['name']) # # 例如：./batch/* -> ./batch/batch_data_0, ./batch/*.json -> ./batch/batch_data_0.json
        if 'No path' not in self.content['path']:
            os.makedirs(self.content['path'], exist_ok=True)  # 创建目录，如果目录已存在则不报错

class superDistillation():
    def __init__(self,teacher_api='', student_model_path='No path', ckpt_dir_lora='No path', task='task0', raw_data_path="oss://ly-llm/data_self_evo/data_agent/train", prompt_template=None):
        # self.stages = ['raw', 'batch', 'distillation', 'train', 'val', 'gt', 'error_example', 'error_example_gt']
        # 初始化数据集目录
        self.current_dir = self.create_directories(['tmp'])
        self.data_dir = self.current_dir + '/data'  # 数据集目录
        self.model_dir = self.current_dir + '/models'  # 模型目录
        os.makedirs(self.data_dir, exist_ok=True)  # 确保数据集目录存在
        os.makedirs(self.model_dir, exist_ok=True)  # 确保
        self.task = task  # 任务名称
        if task is None:
            self.task = f"task{self.count_subfolders('Tasks')}"  # 计算当前目录下的任务数目
        self.work_dir = f"{self.current_dir}/Tasks/{self.task}"  # 工作目录
        os.makedirs(self.work_dir, exist_ok=True)  # 确保工作目录存在
        self.cheacpoint_dir = f"{self.work_dir}/checkpoints"  # 检查点目录
        os.makedirs(self.cheacpoint_dir, exist_ok=True)  # 确保检查点目录存在
        self.raw_data_path = raw_data_path
        self.prompt_template = [prompt_template]
        self.errors = []
        self.root = superData(path='No path', task=self.task, stage='ROOT', description='root')
        self.datas = {
            # "ROOT":superData(path='No path', task=self.task, stage='ROOT', description='root'),
        }
        self.models = {
            "TEACHER": superModel(path='No path', task=self.task, stage='TEACHER', description='ds-v3-teacher', content={'api': teacher_api}, fathers=[self.root]),
            "BASE_LORA": superModel(path=ckpt_dir_lora, task=self.task, stage='BASE_LORA', description='oxs6-cheackpoint-60', fathers=[self.root]),
            "BASE": superModel(path=student_model_path, task=self.task, stage='BASE', description='qwen3-32b-fp32', fathers=[self.root]),
            # "base_trian": superModel(path=ckpt_dir_lora, task=self.task, stage='TRAIN_LORA', description='trained-lr=3e-5-epoch=1'),
        }
        print(f"任务名称: {self.task}")
        print(f"当前工作目录: {self.current_dir}")
        print(f"数据集目录: {self.data_dir}")
        print(f"模型目录: {self.model_dir}")
        print(f"工作目录: {self.work_dir}")
        print(f"检查点目录: {self.cheacpoint_dir}")
        print(f"原始数据路径: {self.raw_data_path}")

    def save_checkpoint(self, path, cp_name, stage_single=True):
        """
        保存当前任务的检查点数据到指定路径。
        """
        # 对于存在于datas和models中的每个stage，如果某阶段有多个，则仅保留记录在案的节点，删除其他节点。
        if stage_single:
            for stage in list(self.datas.keys()):
                for node in self.root.node_list.nodes:
                    if stage == node.content['stage']:
                        if self.datas[stage] != node:
                            node.delete_node()  # 删除当前数据集节点
            for stage in list(self.models.keys()):
                for node in self.root.node_list.nodes:
                    if stage == node.content['stage']:
                        if self.models[stage] != node:
                            node.delete_node()  # 删除当前数据集节点
        num = self.count_files_with_pattern(path, f"checkpoint_{self.task}_{cp_name}")  # 统计当前检查点文件数量
        self.root.save_all(path+f"/checkpoint_{self.task}_{cp_name}_{num}.json")  # 保存数据集族谱
        print(f"保存检查点数据成功: {path}"+f"/checkpoint_{self.task}_{cp_name}_{num}.json")

    def load_cheakpoint(self, path):
        """
        从指定路径加载检查点数据。
        """
        cp = superData.load_all(self, path)  # 从文件中加载数据集族谱
        for node in cp.node_list.nodes:
            if 'ROOT' in node.content['stage']:
                self.root = node
            elif 'data' in node.content['name']:
                self.datas[node.content['stage']] = node
            elif 'model' in node.content['name']:
                self.models[node.content['stage']] = node
        print(f"加载检查点数据成功: {path}")

    @staticmethod
    def count_files_with_pattern(path, pattern):
        """统计包含指定模式的文件数量"""
        # 使用通配符搜索
        search_pattern = os.path.join(path, f"*{pattern}*")
        matching_files = glob.glob(search_pattern)
        
        return len(matching_files)
    
    def count_subfolders(self, folder_name='Tasks'):
        """
        返回 self.current_dir + '/Tasks' 目录下的文件夹数目
        """
        tasks_dir = os.path.join(self.data_dir, folder_name)
        if not os.path.exists(tasks_dir):
            print(f"{tasks_dir} 不存在，正在创建...")
            os.makedirs(tasks_dir, exist_ok=True)  # 如果目录不存在，则创建目录
            print(f"{tasks_dir} 创建成功")

        return len([item for item in os.listdir(tasks_dir) if os.path.isdir(os.path.join(tasks_dir, item))])

    def create_directories(self, directories):
        # 获取并打印当前工作目录
        current_dir = os.getcwd()
        while current_dir == '\n':
            current_dir = input(f"当前文件夹位置: {current_dir}是否正确？正确请回车，不正确请输入正确路径，输入exit结束。")
            # 检查路径是否存在
            if not os.path.exists(current_dir):
                print(f"当前路径 {current_dir} 不存在，请检查路径是否正确。")
                continue
            if current_dir.lower() == 'exit':
                print("程序结束，创建目录失败，请调用create_directories(self, directories)或手动创建。")
                return
        # 跳过路径前后的空格
        current_dir = current_dir.strip()
        # 创建目录
        print("\n开始创建目录...")
        for dir_name in directories:
            dir_path = os.path.join(current_dir, dir_name)
            try:
                # 使用 exist_ok=True 避免目录已存在时报错
                os.makedirs(dir_path, exist_ok=True)
                print(f"✓ 已创建目录: {dir_name}")
            except Exception as e:
                print(f"✗ 创建目录 {dir_name} 失败: {e}")

        print("\n目录创建完成！")

        # 列出当前目录下的所有文件和文件夹
        print("\n当前目录内容:")
        for item in os.listdir(current_dir):
            item_path = os.path.join(current_dir, item)
            if os.path.isdir(item_path):
                print(f"📁 {item}/")
            else:
                print(f"📄 {item}")
        return current_dir

    def action_example(self, data_example):
        """
        示例操作，打印数据集示例内容。
        """
        data_example.save_all()
        data_new = superData(path=os.path.join(self.data_dir, 'raw/*'), task=self.task, stage='RAW', description='nums=3e4+type=multi_step', fathers=[], content={"from_path": 'from_example'})  # 创建原始数据集对象
        self.fun_example(data_example.content["path"], data_new)
        return data_new

    def get_raw(self, from_path):
        """
        从指定路径加载原始数据集。
        """
        raw_data = superData(path=os.path.join(self.data_dir, 'raw/*/'), task=self.task, stage='RAW', description='', fathers=[self.root], content={"from_path": from_path})  # 创建原始数据集对象
        self.datas[raw_data.content['stage']] = raw_data  # 将原始数据集添加到数据集中
        self.execute_ossutil_method1(from_path, raw_data.content["path"]) # 从OSS下载原始数据
        self.save_checkpoint(self.cheacpoint_dir, 'get_raw')  # 保存当前任务进度
    
    def raw2batch(self, raw_data, model_name='deepseek-v3', ratio=[0, 1], sample_num=1e2, single_file_nums=1e2, ans_num=1):
        """
        将原始数据集转换为批处理数据集。
        """
        from tools.raw2batch import raw2batch
        # 创建批处理数据集对象
        batch_data = superData(path=self.data_dir + '/batch/*', task=self.task, stage='BATCH', description=f"{sample_num}-{single_file_nums}-{ratio[0]}-{ratio[1]}-{model_name}", fathers=[raw_data])  
        self.datas[batch_data.content['stage']] = batch_data  # 将原始数据集添加到数据集中
        # 调用func1方法，将原始数据集转换为批处理数据集
        raw2batch(
            raw_data=raw_data,
            batch_data = batch_data,
            model_name=model_name,
            ratio=ratio, 
            sample_num=sample_num, 
            single_file_nums=single_file_nums,
            ans_num=ans_num
        )  # 默认使用func1方法
        self.save_checkpoint(self.cheacpoint_dir, 'raw2batch')  # 保存当前任务进度
    
    def batch2distill(self, batch_data, thread_count=2, type='batch', continue_flag=False):
        from tools.batch2distill import batch2distill, multi_thread_batch2distill
        from tools.single2distill import single2distill, multi_thread_single2distill
        """        
        将批处理数据集转换为蒸馏数据集。
        """
        distill_data = superData(path=self.data_dir + '/distillation/*', task=self.task, stage='DISTILLATION', description=f"{type}{thread_count}thread", fathers=[batch_data])  # 创建蒸馏数据集对象
        distill_error_data = superData(path=self.data_dir + '/distillation_error/*', task=self.task, stage='DISTILLATION_ERROR', description=f"{type}{thread_count}thread", fathers=[batch_data])  # 创建蒸馏错误数据集对象
        self.datas[distill_data.content['stage']] = distill_data  # 将蒸馏数据集添加到数据集中
        self.datas[distill_error_data.content['stage']] = distill_error_data  # 将蒸馏错误数据
        if type == 'single':
            if thread_count == 1:
                single2distill(
                    batch_path=batch_data.content['path'],
                    distill_path=distill_data.content['path'],
                    continue_flag=continue_flag,  # 是否继续上次的转换
                )
            else:
                multi_thread_single2distill(
                    batch_path=batch_data.content['path'],
                    distill_path=distill_data.content['path'],
                    error_file_path=distill_error_data.content['path'],
                    thread_count=thread_count,  # 默认使用2个线程进行批处理数据集转换
                    continue_flag=continue_flag  # 是否继续上次的转换
                )
        elif type == 'batch':  # 默认使用batch2distill方法进行批处理数据集转换
            if thread_count == 1:
                batch2distill(
                    batch_path=batch_data.content['path'],
                    distill_path=distill_data.content['path'],
                    error_file_path=distill_error_data.content['path'],
                    start=0,
                    end=2  # 默认使用全部数据  
                    )
            else:
                multi_thread_batch2distill(
                    batch_path=batch_data.content['path'],
                    distill_path=distill_data.content['path'],
                    error_file_path=distill_error_data.content['path'],
                    thread_count=thread_count  # 默认使用2个线程进行批处理数据集转换
                )  # 调用batch2distill方法
        else:
            print(f"未定义的转换类型: {type}，请使用'single'或'batch'。")
            del self.datas[distill_data.content['stage']]  # 删除蒸馏数据集
            del self.datas[distill_error_data.content['stage']]  # 删除蒸
        self.save_checkpoint(self.cheacpoint_dir, 'batch2distill')  # 保存当前任务进度
        
    def distill2train(self, raw_data, batch_data, distill_data):
        from tools.distill2train import distill2train
        """
        将批处理数据集转换为训练数据集。
        """
        # 创建训练和测试数据集
        train_data = superData(path=self.data_dir + '/train/*', stage='TRAINING', discribe='Train data from batch and distillation data', fathers=[raw_data, batch_data, distill_data])  # 创建训练数据集对象
        test_data = superData(path=self.data_dir + '/test/*', stage='TESTING', discribe='Test data from batch and distillation data', fathers=[raw_data, batch_data, distill_data])  # 创建测试数据集对象
        self.datas[train_data.content['stage']] = train_data  # 将训练数据集添加到数据集中
        self.datas[test_data.content['stage']] = test_data  # 将测试数据集添加到数据集中
        distill2train(
            raw_path=raw_data.path,
            batch_path=batch_data.path,
            distill_path=distill_data.path,
            train_path=train_data.path,
            test_path=test_data.path,
            test_ratio=0.05,  # 默认使用0.05的测试比例
            raw_ratio=0.  # 默认使用0.1的原始数据比例
        )
        self.save_checkpoint(self.cheacpoint_dir, 'distill2train')  # 保存当前任务进度

    def train(self, train_data, base, base_lora):
        """
        使用蒸馏数据集训练学生模型。
        """
        from tools.train import train
        self.models['final_student'] = self.model_dir + '/train_models/'  # 更新学生模型路径
        trained_model = superModel(path=self.model_dir + '/train_models/*', stage='TRAINED', discribe='Trianed student model from distillation data', fathers=[train_data, base, base_lora])  # 创建训练后的学生模型数据集对象
        self.models[trained_model.content['stage']] = trained_model  # 将原始数据集添加到数据集中
        train(
            model_path=base.content['path'],  # 学生模型路径
            ckpt_dir_lora=base_lora.content['path'],  # LoRA检查点目录，暂时未定义
            dataset=train_data.content['path'],  # 蒸馏数据集路径
            output_dir=trained_model.content['path'],  # 输出目录
        )
        print(f"Training student model at {base.content['path']} and {base_lora.content['path']} with data from {train_data.content['path']}")
        self.save_checkpoint(self.cheacpoint_dir, 'train')  # 保存当前任务进度

    @staticmethod
    def clear_folder(folder_path):
        for filename in os.listdir(folder_path):
            file_path = os.path.join(folder_path, filename)
            if os.path.isfile(file_path) or os.path.islink(file_path):
                os.remove(file_path)
            elif os.path.isdir(file_path):
                shutil.rmtree(file_path)

    @staticmethod
    def super_split(text, separators):
        import re
        # text = "some text```sql SELECT * FROM table```python print('hello')``` more text"
        # separators = ['```sql', '```python', '```']

        # 创建正则表达式模式
        pattern = '|'.join(re.escape(sep) for sep in separators)
        result = re.split(pattern, text)

        # 过滤空字符串
        return [item.strip() for item in result if item.strip()]
        
    @staticmethod
    def data_load(data_path, layers = 0, excludes=None, filters=None, mode=None):
        """
        从指定路径加载数据集。支持json、jsonl文件或包含json/jsonl文件的文件夹。
        返回：list（每条数据为dict）
        """
        import glob

        def load_json(fp):
            with open(fp, 'r', encoding='utf-8') as f:
                return json.load(f)

        def load_jsonl(fp):
            data = []
            with open(fp, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        data.append(json.loads(line))
            return data

        all_data = []

        # OSS 路径处理
        if data_path.startswith("oss://"):
            superDistillation.execute_ossutil_method1(data_path, os.path.join(superDistillation.current_dir, 'tmp'))
            data_path = os.path.join(superDistillation.current_dir, 'tmp')

        # 文件夹
        if os.path.isdir(data_path):
            files = glob.glob(os.path.join(data_path, "*.json")) + glob.glob(os.path.join(data_path, "*.jsonl"))
            while layers > 0:
                # 递归加载指定层数的子目录
                files = glob.glob(os.path.join(data_path, "*"))
                for fp in files:
                    fp_ = os.path.join(data_path, fp)
                    if os.path.isdir(fp_):
                        files += glob.glob(os.path.join(fp_, "*.json")) + glob.glob(os.path.join(fp_, "*.jsonl"))
                layers -= 1
            files = [file for file in files if file.endswith('.json') or file.endswith('.jsonl')]
            # files = glob.glob(os.path.join(data_path, "*.json")) + glob.glob(os.path.join(data_path, "*.jsonl"))
            for fp in tqdm(files):
                if excludes and any(exclude in fp for exclude in excludes):
                    print(f"跳过文件 {fp}，因为它不符合预期：{excludes}")
                    continue
                if filters and any(filter not in fp for filter in filters):
                    print(f"跳过文件 {fp}，因为它不符合预期：{filter}")
                    continue
                if fp.endswith('.json'):
                    d = load_json(fp)
                    if isinstance(d, list):
                        all_data.extend(d)
                    else:
                        all_data.append(d)
                elif fp.endswith('.jsonl'):
                    all_data.extend(load_jsonl(fp))
        # 单文件
        elif os.path.isfile(data_path):
            if mode == 'json' or data_path.endswith('.json'):
                d = load_json(data_path)
                if isinstance(d, list):
                    all_data.extend(d)
                else:
                    all_data.append(d)
            elif mode == 'jsonl' or data_path.endswith('.jsonl'):
                all_data.extend(load_jsonl(data_path))
            else:
                raise ValueError(f"不支持的文件类型: {data_path}")
        else:
            raise FileNotFoundError(f"数据集路径 {data_path} 不存在！")

        return all_data

    @staticmethod
    def data_save(data, save_path, mode=None, save_type='w'):
        """
        保存数据到指定路径。支持保存为 json 或 jsonl 格式。
        参数:
            data: list 或 dict，待保存的数据
            save_path: str，保存路径
            mode: 'json' 或 'jsonl'
        """
        import json
        if save_type not in ['w', 'a']:
            raise ValueError("save_type 只能为 'w' 或 'a'，分别表示写入和追加模式。")
        elif save_type == 'a' and not os.path.exists(save_path):
            print(f"文件 {save_path} 不存在，创建新文件。")
        
        os.makedirs(os.path.dirname(save_path), exist_ok=True)
        if not mode:
            mode = 'json' if save_path.endswith('.json') else 'jsonl' if save_path.endswith('.jsonl') else None

        if mode == 'json':
            with open(save_path, save_type, encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
        elif mode == 'jsonl':
            with open(save_path, save_type, encoding='utf-8') as f:
                if isinstance(data, dict):
                    f.write(json.dumps(data, ensure_ascii=False) + '\n')
                else:
                    for item in data:
                        f.write(json.dumps(item, ensure_ascii=False) + '\n')
        else:
            raise ValueError("mode 只能为 'json' 或 'jsonl'")

    @staticmethod
    def jsonl_json_swift(path, mode='json2jsonl'):
        """
        将 jsonl 文件转换为 json 文件，或将 json 文件转换为 jsonl 文件。
        """
        import json
        import os

        if not os.path.exists(path):
            raise FileNotFoundError(f"文件 {path} 不存在！")

        if mode == 'json2jsonl':
            # 将 json 文件转换为 jsonl 文件
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            jsonl_path = path.replace('.jsonl', '.json').replace('.json', '.jsonl')
            with open(jsonl_path, 'w', encoding='utf-8') as f:
                for item in data:
                    f.write(json.dumps(item, ensure_ascii=False) + '\n')
            print(f"已将 {path} 转换为 {jsonl_path}")
            return jsonl_path
        elif mode == 'jsonl2json':
            # 将 jsonl 文件转换为 json 文件
            with open(path, 'r', encoding='utf-8') as f:
                data = [json.loads(line.strip()) for line in f if line.strip()]
            json_path = path.replace('.jsonl', '.json')
            with open(json_path, 'w', encoding='utf-8') as f:   
                json.dump(data, f, ensure_ascii=False, indent=4)
            print(f"已将 {path} 转换为 {json_path}")
            return json_path
        else:
            raise ValueError("mode 只能为 'json2jsonl' 或 'jsonl2json'，分别表示将 json 转换为 jsonl 或将 jsonl 转换为 json。")

    def execute_ossutil_method1(self, data_from, data_to):
        """使用subprocess.run()执行ossutil命令"""
        command = [
            "ossutil",
            "cp",
            "-r",
            data_from,
            data_to,
            "-c",
            self.current_dir+"/script/oss2xy.sh"
        ]
        if data_from.split('/')[-1] in os.listdir(data_to):
            print(f"数据 {data_from} 已经存在于 {data_to}，跳过拉取。")
            return 0
        try:
            # 执行命令并捕获输出
            print('开始从 OSS 拉取数据')
            result = subprocess.run(
                command,
                capture_output=True,
                text=True,
                check=True  # 如果命令失败会抛出异常
            )
            
            print("命令执行成功！")
            print("标准输出:", result.stdout)
            if result.stderr:
                print("标准错误:", result.stderr)
            
            return result.returncode
            
        except subprocess.CalledProcessError as e:
            print(f"命令执行失败！返回码: {e.returncode}")
            print(f"错误输出: {e.stderr}")
            return e.returncode
