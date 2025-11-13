import aio_pika
import asyncio
import json
import time
import tempfile
from src.analyze.s3_utils import download_file_from_s3, upload_json_to_s3
from src.analyze.redis_utils import set_task_status
from src.config import (
    RABBITMQ_HOST,
    RABBITMQ_PORT,
    RABBITMQ_USER,
    RABBITMQ_PASSWORD,
    RABBITMQ_QUEUE_NAME,
    S3_BUCKET_NAME,
    S3_ENDPOINT_URL
)


async def handle_message(message: aio_pika.IncomingMessage):
    async with message.process():
        data = json.loads(message.body.decode())
        task_id = data["task_id"]
        file_key = data["file_s3_key"]

        # 1. Создаём временный путь для файла
        tmp_file_path = tempfile.mktemp(suffix=".tmp")

        print(f"⬇️ Downloading file for task {task_id}...")
        download_file_from_s3(file_key, tmp_file_path)

        # 2. Имитация долгого анализа
        print(f"⏳ Processing task {task_id}...")
        await asyncio.sleep(15)  

        # 3. Загружаем результат в S3
        result_key = f"results/{task_id}.json"
        result_data = {"film": "sosi", "status": "xuy"}
        upload_json_to_s3(result_data, result_key)
        result_key = f"{S3_ENDPOINT_URL}/{S3_BUCKET_NAME}/results/{task_id}.json"
        print(f"✅ Task {task_id} done, result uploaded to S3: {result_key}")

        # 4. Обновляем статус в Redis
        await set_task_status(task_id, {"status": "complited", "result_key": result_key})
        print(f"🔄 Task {task_id} status updated in Redis")



async def main():
    connection = await aio_pika.connect_robust(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        login=RABBITMQ_USER,
        password=RABBITMQ_PASSWORD
    )
    channel = await connection.channel()
    queue = await channel.declare_queue(RABBITMQ_QUEUE_NAME, durable=True)
    await queue.consume(handle_message)
    print("🚀 Worker is running and waiting for tasks...")

    # Удерживаем процесс живым
    try:
        while True:
            await asyncio.sleep(1)
    except KeyboardInterrupt:
        print("🔴 Worker stopped manually")
        await connection.close()


if __name__ == "__main__":
    asyncio.run(main())
