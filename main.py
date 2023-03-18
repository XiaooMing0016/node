import json
import logging
import time
from typing import Dict
import requests
import uvicorn
from fastapi import FastAPI, HTTPException, Request
import asyncio
import random

app = FastAPI()

# 设置日志记录器
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

_tasks: Dict[str, Dict] = {
    'task_id': {
        'task_node': {
            'task_id': 'task_id',  # 任务id
            'task_node': 'task_node',  # 任务节点
            'task_name': 'task_name',  # 任务名称
            'task_type': 'task_type',  # 任务类型
            'task_type_name': 'task_type_name',  # 任务类型名称
            'task_priority': 'task_priority',  # 任务优先级
            'task_destination': 'task_destination',  # 任务目的地
            'task_status': 'task_status',  # 任务状态
            'task_progress': 'task_progress',  # 任务进度
            'task_start_time': 'task_start_time',  # 任务开始时间
            'task_end_time': 'task_end_time',  # 任务结束时间
        }
    }
}


# 协程任务
async def task(ip, task_id: str, node_id: str, task_priority: str, task_type_name: str):
    logger.info(f"Start task {task_id} {task_type_name} on node {node_id}")
    logger.info(f"Task {task_id} on node {node_id} is running")
    for i in range(125):
        if _tasks[task_id][node_id]['task_status'] == 'stop':
            logger.info(f"Task {task_id} on node {node_id} is stopped")
            break
        try: 
            response = requests.get(f"http://{ip}/task/process/{task_id}/{node_id}/{i}")
            if response.status_code == 200:
                logger.info(f"Task {task_id} on node {node_id} is processing, number {str(i)}, total 125, "
                            f"progress {str(i / 125 * 100)}%, priority {task_priority}")
            else:
                logger.error(f"Task {task_id} on node {node_id} is failed")
        except Exception as e:
            logger.info(f"Task {task_id} on node {node_id} is failed: {e}")
        # 随机等待3~5秒
        await asyncio.sleep(random.randint(3, 5))
    logger.info(f"Task {task_id} on node {node_id} is completed")
    # 任务完成后，将任务状态改为已完成
    try:
        response = requests.get(f"http://{ip}/task/finish/{task_id}/{node_id}")
        if response.status_code == 200:
            logger.info(f"Task {task_id} on node {node_id} is completed")
        else:
            logger.error(f"Task {task_id} on node {node_id} is failed")
    except Exception as e:
        logger.error(f"Task {task_id} on node {node_id} is failed: {e}")


@app.get("/")
async def root():
    try:
        # 从json文件中读取更新task
        with open('tasks.json', 'r') as f:
            file_tasks = json.load(f)
        for task_id in file_tasks:
            _tasks[task_id] = file_tasks[task_id]
    except Exception as e:
        logger.warning(f"Failed to read tasks.json: {e}")
    return {"message": "Hello World"}


# 注册节点
@app.get("/register/{token}")
async def register_node(token: str):
    try:
        response = requests.get(f"http://34.130.234.56/register/{token}")
        if response.status_code == 200:
            logger.info(f"Register node successfully")
            return {"message": "Register node successfully"}
        else:
            logger.warning(f"Register node failed")
            return {"message": "Register node failed", "error": response.text}
    except Exception as e:
        logger.error(f"Register node failed: {e}")


# 注销节点
@app.get("/unregister/{token}")
async def unregister_node(token: str):
    try:
        response = requests.get(f"http://34.130.234.56/unregister/{token}")
        if response.status_code == 200:
            logger.info(f"Unregister node successfully")
            return {"message": "Unregister node successfully"}
        else:
            logger.warning(f"Unregister node failed")
            return {"message": "Unregister node failed", "error": response.text}
    except Exception as e:
        logger.error(f"Unregister node failed: {e}")
        return {"message": "Unregister node failed", "error": e}


# 创建任务
@app.get("/task/init/{task_type_name}/{task_id}/{node_id}/{task_name}/{priority}")
async def init_task(request: Request, task_type_name: str, task_id: str, node_id: str, task_name: str, priority: str):
    if task_id not in _tasks:
        logger.info(f"Received task {task_id}，start task")
        _tasks[task_id] = {}
        _tasks[task_id][node_id] = {"task_id": task_id, "task_node": node_id,
                                    "task_name": task_name, "task_type_name": task_type_name,
                                    "task_priority": priority, "task_status": "created",
                                    "creat_time": (time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))}
        # 启用协程，启动任务
        _tasks[task_id][node_id] = {"task", asyncio.create_task(task(request.client.host, task_id, node_id, priority, task_type_name))}
        # 将任务信息写入json文件
        with open('tasks.json', 'w') as f:
            json.dump(_tasks, f)
        return {"message": "task finished"}
    else:
        logger.warning(f"task_id already exists: {task_id}")
        raise HTTPException(status_code=400, detail="task_id already exists")


# 查询任务状态
@app.get("/task/status/{task_id}")
async def get_task_status(task_id: str):
    if task_id in _tasks:
        return {_tasks[task_id]}
    else:
        return {'message': 'task_id does not exist'}


# 停止任务
@app.get("/task/stop/{task_id}")
async def stop_task(task_id: str):
    if task_id in _tasks:
        _tasks[task_id]["task_status"] = "stop"
        _tasks[task_id][node_id]["task"].cancel()
        return {"message": "stop task successfully"}
    else:
        return {'message': 'task_id does not exist'}


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
