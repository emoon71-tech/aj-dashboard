import json


def build_engine(code: str):
    result = {
        "code": code,
        "quote": {},
        "technical": {},
        "chips": {},
        "score": {},
        "decision": {}
    }
    return result


if __name__ == "__main__":
    code = "2330"
    engine = build_engine(code)

    with open("engine_2330.json", "w", encoding="utf-8") as f:
        json.dump(engine, f, ensure_ascii=False, indent=2)

    print("✅ 已建立 engine_2330.json")
