import json
import os
import re
from pathlib import Path


def has_japanese_or_korean_chars(a: str) -> bool:
    # Match any character in the Unicode block for Japanese or Korean characters
    result = bool(re.search(r'[\u3040-\u30ff\uac00-\ud7a3]', a.replace('の', '的')) or re.search(
        r'(日南|真琴|天知遥|秋水|养猪妹|利香|幽灵妹|柚木)', a))
    return result


def get_files_in_dir(dir_path):
    file_list = []
    for root, dirs, files in os.walk(dir_path):
        for file_name in files:
            if has_japanese_or_korean_chars(file_name):
                file_list.append(os.path.join(root, file_name))
    return file_list

def del_files_in_dir(dir_path):
    file_list = []
    for root, dirs, files in os.walk(dir_path):
        for file_name in files:
            if has_japanese_or_korean_chars(file_name):
                file_full_name = os.path.join(root, file_name)
                file_path = Path(file_full_name)
                file_path.unlink()
                file_list.append(file_full_name)
                print(f"{file_full_name} deleted")
    return file_list


def save_list_to_json(list, json_file):
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(list, f, indent=4, ensure_ascii=False)
def main():
    root_path = '/Users/wuyun/Downloads/mnt/onedrive/docu/telegram/Asmr'
    del_list_json = '/Users/wuyun/Downloads/Other/del_list.json'
    save_list_to_json(del_files_in_dir(root_path), del_list_json)


if __name__ == "__main__":
    main()