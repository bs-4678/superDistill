import subprocess

def train(model_path, dataset, output_dir, ckpt_dir_lora):
    # model_path = "/mnt/model/Qwen3-32B"
    # dataset = "/mnt/wujinyi/train_demo/data/distill5_5e4_sft_twostep.jsonl"
    # output_dir = "/mnt/wujinyi/train_demo/output"
    # ckpt_dir_lora = "/mnt/wujinyi/train_demo/models/checkpoint-60"
    cmd = f"""
    export model_path={model_path}
    export dataset={dataset}
    export output_dir={output_dir}
    export ckpt_dir_lora={ckpt_dir_lora}
    export CUDA_VISIBLE_DEVICES=0,1,2,3
    export NPROC_PER_NODE=4

    swift sft \
        --attn_impl flash_attn \
        --model $model_path \
        --model_type qwen3 \
        --train_type lora \
        --dataset $dataset \
        --torch_dtype bfloat16 \
        --num_train_epochs 1 \
        --per_device_train_batch_size 4 \
        --per_device_eval_batch_size 1 \
        --learning_rate 5e-5 \
        --lora_rank 8 \
        --lora_alpha 32 \
        --target_modules all-linear \
        --gradient_accumulation_steps 8 \
        --eval_steps 100 \
        --save_steps 100 \
        --save_total_limit 2 \
        --logging_steps 5 \
        --max_length 2048 \
        --output_dir $output_dir \
        --warmup_ratio 0.05 \
        --dataloader_num_workers 0 \
        --dataset_num_proc 1  \
        --save_only_model true \
        --use_liger_kernel true \
        --report_to tensorboard \
        --resume_only_model True \
        --resume_from_checkpoint $ckpt_dir_lora \
        --deepspeed zero2
    """

    subprocess.run(cmd, shell=True, check=True, executable="/bin/bash")