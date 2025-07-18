import os,random,json,traceback,re,sys
from copy import deepcopy
from tqdm import tqdm
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from superDistill import superDistillation

class myDistill(superDistillation):
    def __init__(self, teacher_api='', student_model_path='No path', ckpt_dir_lora='No path', task='task0', raw_data_path="oss://ly-llm/data_self_evo/data_agent/train", prompt_template=None):
        super().__init__(teacher_api=teacher_api, student_model_path=student_model_path, raw_data_path=raw_data_path, task=task, ckpt_dir_lora=ckpt_dir_lora, prompt_template=prompt_template)

    def my_workflow(self):
        """
        演示工作流：从数据采样、蒸馏、训练到验证。
        """
        os.makedirs(self.work_dir, exist_ok=True)
        print(f"工作目录已创建: {self.work_dir}")

        print("Starting workflow...")
        self.load_cheakpoint('/Users/wujinyi/Desktop/mycode/superDistill/Tasks/official_version/get_raw_v1.json')
        # self.load_cheakpoint('/Users/wujinyi/Desktop/mycode/superDistill/Tasks/official_version/batch2distill_v1.json')
        # self.get_raw(self.raw_data_path)
        self.raw2batch(raw_data=self.datas['RAW'], model_name='deepseek-v3', ratio=[0, 1], sample_num=40000, single_file_nums=4000)
        self.batch2distill(batch_data=self.datas['BATCH'], thread_count=30, type='single', continue_flag=True)
        # self.distill2train(self.datas['RAW'], self.datas['BATCH'], self.datas['DISTILLATION'], test_ratio=0.05, raw_ratio=0.)
        # self.train(self.datas['TRAIN'], self.models['BASE'], self.models['BASE_LORA'], train_type='train_gpus0-4_5e-5_4x4x8_ep1')
        # self.datas['test_score'] = self.model_eval(self.datas['test'])
        # self.datas['gt'] = self.get_ground_truth()
        # self.datas['gt_score'] = self.model_eval(self.datas['gt'])
        # self.datas['error_example_test'] = self.collect_error_samples(self.datas['test_score'])
        # self.datas['error_example_gt'] = self.collect_error_samples_gt(self.datas['error_example_gt'])
        print("Workflow completed.")

    def my_workflow_continue(self):
        """
        演示工作流：从数据采样、蒸馏、训练到验证。
        """
        os.makedirs(self.work_dir, exist_ok=True)
        print(f"工作目录已创建: {self.work_dir}")

        print("Starting workflow...")
        self.get_raw(self.raw_data_path)
        self.load_cheakpoint()
        self.raw2batch(self.datas['RAW'])
        # self.batch2distill(self.datas['BATCH'])
        # self.distill2train(self.datas['RAW'], self.datas['batch'], self.datas['distill'])
        # self.train(self.datas['train'], self.models['base'], self.models['base_lora'])
        # self.datas['test_score'] = self.model_eval(self.datas['test'])
        # self.datas['gt'] = self.get_ground_truth()
        # self.datas['gt_score'] = self.model_eval(self.datas['gt'])
        # self.datas['error_example_test'] = self.collect_error_samples(self.datas['test_score'])
        # self.datas['error_example_gt'] = self.collect_error_samples_gt(self.datas['error_example_gt'])
        print("Workflow completed.")

    def run(self):
        """
        执行自定义的蒸馏流程。
        """
        print("Running myDistill process...")
        self.my_workflow()
    
if __name__ == "__main__":
    # 示例：创建 myDistill 实例并运行
    teacher_api = "http://example.com/api"  # 替换为实际的教师API地址
    student_model_path = "/Users/wujinyi/Desktop/mycode/superDistill/tmp"  # 替换为实际的学生模型路径
    raw_data_path = "oss://ly-llm/data_self_evo/data_agent/train"  # 替换为实际的原始数据路径
    prompt_template = "Your prompt template here"  # 替换为实际的提示模板
    task = "prompt_test1"  # 替换为实际的任务名称

    # distiller = myDistill(teacher_api=teacher_api, student_model_path=student_model_path, ckpt_dir_lora=student_model_path, task=task, raw_data_path=raw_data_path, prompt_template=prompt_template)
    distiller = myDistill(task='distill_v2')
    distiller.run()
    
    # nohup python /Users/wujinyi/Desktop/mycode/superDistill/workflow/myDistill.py > /Users/wujinyi/Desktop/mycode/superDistill/workflow/output/run1.log 2>&1 &