import json

with open(r"C:\Users\Admin\Downloads\Telegram Desktop\PT1.parsed.json", "r", encoding="utf-8") as f:
    json_content = json.load(f)

print(json_content)