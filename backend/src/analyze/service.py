import uuid
import json
import aio_pika
from fastapi import UploadFile
from src.analyze.s3_utils import upload_file_to_s3
from src.analyze.redis_utils import set_task_status, get_task_status
from src.config import RABBITMQ_HOST, RABBITMQ_PORT, RABBITMQ_USER, RABBITMQ_PASSWORD, RABBITMQ_QUEUE_NAME

async def process_file(file: UploadFile) -> str:
    task_id = str(uuid.uuid4())
    s3_key = f"uploads/{task_id}_{file.filename}"

    # Загружаем файл в S3
    await upload_file_to_s3(file, s3_key)

    # Помечаем задачу как pending в Redis
    await set_task_status(task_id, {"status": "pending", "result_key": None})

    # Публикуем задачу в RabbitMQ
    message = {"task_id": task_id, "file_s3_key": s3_key, "filename": file.filename}
    await publish_to_rabbitmq(message)

    return task_id

async def publish_to_rabbitmq(message: dict):
    connection = await aio_pika.connect_robust(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        login=RABBITMQ_USER,
        password=RABBITMQ_PASSWORD
    )
    async with connection:
        channel = await connection.channel()
        await channel.declare_queue(RABBITMQ_QUEUE_NAME, durable=True)
        await channel.default_exchange.publish(
            aio_pika.Message(
                body=json.dumps(message).encode(),
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT
            ),
            routing_key=RABBITMQ_QUEUE_NAME
        )

# Получение статуса задачи
async def check_task_status(task_id: str) -> dict:
    return await get_task_status(task_id)