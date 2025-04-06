from concurrent.futures import ProcessPoolExecutor, as_completed
import time

def worker(func, arg):
    result = func(arg)
    return (arg, result)  # 返回参数作为排序依据

def ordered_parallel_exec(func_args_list, n_workers):
    # 保持原始顺序的ID映射
    order_map = {arg: idx for idx, (_, arg) in enumerate(func_args_list)}
    results = [None] * len(func_args_list)

    with ProcessPoolExecutor(max_workers=n_workers) as executor:
        # 提交任务时携带原始顺序信息
        futures = {
            executor.submit(worker, func, arg): (func, arg)
            for func, arg in func_args_list
        }

        # 按完成顺序收集但按原始顺序存储
        for future in as_completed(futures):
            arg, result = future.result()
            orig_idx = order_map[arg]
            results[orig_idx] = result

    # 按原始顺序打印
    for result in results:
        print(result)

# 示例用法
if __name__ == "__main__":
    def task(x):
        time.sleep(0.5 * x)
        return f"结果_{x}"

    tasks = [(task, i) for i in [3, 1, 2, 4]]
    ordered_parallel_exec(tasks, n_workers=2)
