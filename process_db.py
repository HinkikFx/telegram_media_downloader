import difflib
import json
import os
import re
import shutil
import sys
from pathlib import Path
from time import sleep

from tqdm import tqdm

from module import sqlmodel
from playhouse.shortcuts import model_to_dict
from utils.format import validate_title, guess_media_type, move_file, is_exist_files_with_prefix, process_string

db = sqlmodel.Downloaded()

similar_set = 0.90
sizerange_min = 0.005

if sys.platform.startswith('linux'):
    root_dir = "/mnt/onedrive/"
else:
    root_dir = "/Users/wuyun/Downloads/mnt/onedrive/"


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

def del_file(file_path):
    file_path = Path(file_path)
    for i in range(3):
        try:
            if file_path.exists():
                file_path.unlink()
            sleep(1)
            if not file_path.exists():
                return True
        except Exception as e:
            print (e)
            pass



def find_files_with_prefix(folder_path, prefix):
    file_list = []
    if os.path.exists(folder_path):
        with os.scandir(folder_path) as entries:
            for entry in entries:
                if entry.is_file() and entry.name.startswith(prefix):
                    file_list.append(entry.path)
        return file_list
    else:
        return False

def save_list_to_json(list, json_file):
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(list, f, indent=4, ensure_ascii=False)

def get_aka_msg(msg_dict):
    file_ext = os.path.splitext(msg_dict.get('filename'))[-1]
    file_type = guess_media_type(file_ext)
    if file_type == 'video':
        subdir = os.path.join(root_dir,'cav1/telegram')
    elif file_type == 'audio':
        subdir = os.path.join(root_dir,'docu/telegram/Asmr')
    elif file_type == 'photo':
        subdir = os.path.join(root_dir,'images2/telegram')
    elif file_type == 'document':
        subdir = os.path.join(root_dir,'docu/telegram/Books')
    else:
        subdir = os.path.join(root_dir,'upload/telegram')

    if not msg_dict.get('chat_username'):
        file_dir = os.path.join(subdir,
                                validate_title(f"[{str(msg_dict.get('chat_id'))}]{msg_dict.get('chat_id')}"),
                                str(int(msg_dict.get('message_id')) // 100 * 100).zfill(6))
    else:
        file_dir = os.path.join(subdir,
                                validate_title(f"[{str(msg_dict.get('chat_id'))}]{msg_dict.get('chat_username')}"),
                                str(int(msg_dict.get('message_id')) // 100 * 100).zfill(6))

    folder_path = file_dir
    prefix = f"[{msg_dict.get('message_id')}]"

    return folder_path, prefix


def get_save_dir(file_ext):
    file_type = guess_media_type(file_ext)
    if file_type == 'video':
        save_dir = os.path.join(root_dir,'cav1/telegram')
    elif file_type == 'audio':
        save_dir = os.path.join(root_dir,'docu/telegram/Asmr')
    elif file_type == 'photo':
        save_dir = os.path.join(root_dir,'images2/telegram')
    elif file_type == 'document':
        save_dir = os.path.join(root_dir,'docu/telegram/Books')
    else:
        save_dir = os.path.join(root_dir,'upload/telegram')

    return save_dir

def get_aka_file_dir(msg_dict):
    file_ext = os.path.splitext(msg_dict.get('filename'))[-1]
    subdir = get_save_dir(file_ext)

    if not msg_dict.get('chat_username'):
        file_dir = os.path.join(subdir,
                                validate_title(f"[{str(msg_dict.get('chat_id'))}]{msg_dict.get('chat_id')}"),
                                str(int(msg_dict.get('message_id')) // 100 * 100).zfill(6))
    else:
        file_dir = os.path.join(subdir,
                                validate_title(f"[{str(msg_dict.get('chat_id'))}]{msg_dict.get('chat_username')}"),
                                str(int(msg_dict.get('message_id')) // 100 * 100).zfill(6))

    return file_dir

def get_aka_file_path(msg_dict):
    file_dir = get_aka_file_dir(msg_dict)
    matched_file = os.path.join(file_dir, f"{msg_dict.get('filename')}")

    return matched_file

def load_last_id_json(json_file):
    if os.path.exists(json_file):
        # Read existing data from the file
        with open(json_file, 'r', encoding='utf-8') as f:
            last_id = json.load(f)
        return last_id

def save_last_id_json(json_file, last_id):
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(last_id, f, indent=4, ensure_ascii=False)

def update_json_by_dict(json_file, data_append):
    if os.path.exists(json_file):
        # Read existing data from the file
        with open(json_file, 'r', encoding='utf-8') as f:
            existing_data = json.load(f)
    else:
        # Create an empty dictionary if the file doesn't exist
        existing_data = []
    existing_data.append(data_append)
    # Write the updated data back to the file
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(existing_data, f, indent=4, ensure_ascii=False)

def update_status():
    last_id = load_last_id_json('./temp/last_update_id.json')
    if not last_id or last_id == '':
        last_id = 0
    all_msg = db.get_all_message_from(last_id)
    try:
        for msg in tqdm(all_msg):  # 遍历文件夹
            last_id = msg.id
            file_name_pre = f"{[msg.message_id]}"
            file_path = get_aka_file_dir(model_to_dict(msg))
            if msg.status == 1:
                if not file_path or not is_exist_files_with_prefix(file_path, file_name_pre):
                    msg.status = 4
                    db.msg_update_to_db(model_to_dict(msg))
                    print(f"{msg.filename} is not exist. db updated......")
            else:
                if file_path and is_exist_files_with_prefix(file_path, file_name_pre):
                    msg.status = 1
                    db.msg_update_to_db(model_to_dict(msg))
                    print(f"{msg.filename} is exist. db updated......")

    except KeyboardInterrupt:
        print("KeyboardInterrupt")
    except Exception as e:
        print(e)
    finally:
        print(f"last_id = {last_id}")
        save_last_id_json('./temp/last_update_id.json', last_id)

def del_dulp():

    last_id = load_last_id_json('./temp/last_id.json')
    if not last_id or last_id == '':
        last_id = 0
    all_msg = db.get_all_finished_message_from(last_id)
    try:
        # for msg in tqdm(all_msg):  # 遍历文件夹
        #     if '【' in msg.filename:
        #         filename_only = os.path.splitext(msg.filename)[-2]
        #         print (f"{filename_only}==================={process_string(filename_only)}")

        for msg in tqdm(all_msg): #遍历文件夹
        # for msg in all_msg:  # 遍历文件夹
            last_id = msg.id
            # if not msg.media_duration or msg.media_duration <= 0:
            #     continue
            if db.getStatusById(msg.id) != 1:
                print(f"{msg.filename} is dealed. passed......")
                continue
            simlar_files = db.get_similar_files(model_to_dict(msg), similar_set, sizerange_min) #找到他的相似文件
            if len(simlar_files) > 1: #大于1个 说明有重复文件记录
                remained = False
                for simlar_file in simlar_files:
                    # folder_path, prefix = get_aka_msg(model_to_dict(simlar_file))
                    # file_paths = find_files_with_prefix(folder_path, prefix)
                    file_path = get_aka_file_path(model_to_dict(simlar_file))
                    if not file_path:
                        simlar_file.status = 4
                        db.msg_update_to_db(model_to_dict(simlar_file))
                        print(f"{simlar_file.filename} is not exist. db updated......")
                        continue
                    else:
                        if remained:
                            if os.path.exists(file_path):
                                # del_file(file_path)
                                from_path = os.path.dirname(file_path)
                                save_filename = os.path.basename(file_path)
                                file_ext = os.path.splitext(simlar_file.filename)[-1]
                                subdir = get_save_dir(file_ext)
                                to_path = os.path.join(subdir, '2del', os.path.relpath(from_path, subdir))
                                move_file(from_path,to_path,save_filename)

                            simlar_file.status = 4
                            db.msg_update_to_db(model_to_dict(simlar_file))
                            print(f"{simlar_file.filename} has been moved to 2del dir......")

                        else:
                            if os.path.exists(file_path) and os.path.getsize(file_path) >0:
                                remained = True
                                print(f"\n{simlar_file.filename} has been remained++++")
                            else:
                                simlar_file.status = 4
                                db.msg_update_to_db(model_to_dict(simlar_file))
                                print(f"{simlar_file.filename} is missing!!!!. db updated......")
            elif len(simlar_files) == 1:#1个 说明没有重复文件记录 顺便更新一下存不存在
                simlar_file = simlar_files[0]
                # folder_path, prefix = get_aka_msg(model_to_dict(simlar_file))
                # file_paths = find_files_with_prefix(folder_path, prefix)
                file_path = get_aka_file_path(model_to_dict(simlar_file))
                if not file_path:
                    simlar_file.status = 4
                    db.msg_update_to_db(model_to_dict(simlar_file))
                    print(f"{simlar_file.filename} is not exist. db updated......")
                else:
                    print(f"{simlar_file.filename} has no aka_file......")
    except KeyboardInterrupt:
        print("KeyboardInterrupt")
    except Exception as e:
        print(e)
    finally:
        print(f"last_id = {last_id}")
        save_last_id_json('./temp/last_id.json', last_id)

def main():
    update_status()

if __name__ == "__main__":
    main()