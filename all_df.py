import pandas as pd
import os
import dealing_csv
import time
import connect_pg
import json
from SoloLearn import SoloLearn
from typing import Any

## satria ny sasany string ny sasany integer/float ---> super test

path = "../../Downloads/DW/DATAS EXPORT/DATABASE/Clubdesreducs/edit file/"
for val in os.listdir(path):
    with open(path + val, "r") as file:
        data = json.loads(file.read())
        # print(type(data))
count = 0
for key, value in data.items():
    if len(value) > 2:
        print(f"Find in {val} =====================================> {key}:{value}")
        count += 1
        for elt in range(len(value)):
            if type(data[key][elt]) == str:
                pass
            else:
                try:
                    data[key][elt] = int(value[elt])
                except ValueError:
                    raise Exception("error occurred")
                else:
                    data[elt] = value[elt]
print(count)


with open("../../Downloads/DW/DATAS EXPORT/DATABASE/Clubdesreducs/edit file/dict_json_sololearn_9_essai.txt", "w") as file:
    file.write(json.dumps({key: list(set(value)) for key, value in data.items()}))  # asio len aloha lay set value io rehefa anoratra score

