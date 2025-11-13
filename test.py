import os
import boto3
from botocore.exceptions import NoCredentialsError, ClientError
from dotenv import load_dotenv

# Загружаем .env файл
load_dotenv()

# Настройки из окружения
AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
S3_BUCKET_NAME = os.getenv("S3_BUCKET_NAME")
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")

s3 = boto3.client(
    "s3",
    aws_access_key_id=AWS_ACCESS_KEY_ID,
    aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    region_name=AWS_REGION,
    endpoint_url=S3_ENDPOINT_URL
)

def test_s3_connection():
    try:
        # Проверяем список бакетов (или доступ к конкретному)
        print("📦 Проверяем доступ к S3...")
        s3.head_bucket(Bucket=S3_BUCKET_NAME)
        print(f" Подключение к бакету '{S3_BUCKET_NAME}' успешно!")

        # Тест: создадим и загрузим файл
        test_file_name = r"C:\Users\Admin\Desktop\wink\backend\requirements.txt"
        with open(test_file_name, "w") as f:
            f.write("S3 connection test successful ")

        print("⬆ Загружаем тестовый файл...")
        s3.upload_file(test_file_name, S3_BUCKET_NAME, test_file_name)
        print(" Файл успешно загружен!")

        # Проверим, что файл действительно есть
        print(" Проверяем наличие файла...")
        response = s3.list_objects_v2(Bucket=S3_BUCKET_NAME, Prefix=test_file_name)
        if "Contents" in response:
            print(" Файл найден в хранилище!")
        else:
            print(" Файл не найден — возможны проблемы с правами доступа.")
        
        # Удаляем тестовый файл
        print(" Удаляем тестовый файл...")
        # s3.delete_object(Bucket=S3_BUCKET_NAME, Key=test_file_name)
        print(" Файл удалён, тест завершён успешно!")

    except NoCredentialsError:
        print(" Ошибка: не найдены AWS креденшелы.")
    except ClientError as e:
        print(f"Ошибка при работе с S3: {e}")
    except Exception as e:
        print(f"Неожиданная ошибка: {e}")

def download_file_from_s3(s3_key: str, local_path: str):
    """
    s3_key — имя файла/путь в бакете
    local_path — куда сохранить файл локально
    """
    try:
        print(f"⬇️  Скачиваю '{s3_key}' из бакета '{S3_BUCKET_NAME}'...")
        s3.download_file(S3_BUCKET_NAME, s3_key, local_path)
        print(f"✅ Файл сохранён локально как: {local_path}")
    except ClientError as e:
        print(f"❌ Ошибка при скачивании: {e}")
    except Exception as e:
        print(f"⚠️ Неожиданная ошибка: {e}")

if __name__ == "__main__":
    # test_s3_connection()
    download_file_from_s3(r"C:\Users\Admin\Desktop\wink\backend\requirements.txt", r"C:\Users\Admin\Desktop\wink\backend\downloaded_requirements.txt")
