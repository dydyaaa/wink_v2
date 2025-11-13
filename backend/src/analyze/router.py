from fastapi import APIRouter, UploadFile, File, HTTPException
from src.analyze.service import process_file, check_task_status

router = APIRouter(prefix="/analyze", tags=["Analyze"])

@router.post("/")
async def analyze_endpoint(file: UploadFile = File(...)):
    if not file:
        raise HTTPException(status_code=400, detail="Файл не предоставлен")
    task_id = await process_file(file)
    return {"status": "accepted", "task_id": task_id}

@router.get("/{task_id}/status")
async def task_status(task_id: str):
    """
    Проверяет статус задачи по task_id
    """
    result = await check_task_status(task_id)
    return result