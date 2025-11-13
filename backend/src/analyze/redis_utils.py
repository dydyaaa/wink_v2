from redis.asyncio import Redis
import os
from dotenv import load_dotenv
import json

load_dotenv()

REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB = int(os.getenv("REDIS_DB", 0))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

redis = Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    db=REDIS_DB,
    password=REDIS_PASSWORD,
    decode_responses=True
)

async def set_task_status(task_id: str, data: str):
    """Сохраняет статус задачи в Redis"""
    await redis.set(task_id, json.dumps(data))

async def get_task_status(task_id: str):
    """Получает статус задачи из Redis"""
    status_raw = await redis.get(task_id)
    if status_raw is None:
        return {"task_id": task_id, "status": "pending", "result_key": None}
    return {"task_id": task_id, **json.loads(status_raw)}
