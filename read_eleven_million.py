import connect_pg
import dealing_csv
from collections import Counter
import time

start_time = time.time()

# with open("../../Downloads/DW/DATAS EXPORT/CSV/Kontiki_FR_Enrich1_20191106.csv", "r", encoding="utf-8") as file:
with open("../../Downloads/DW/DATAS EXPORT/Kontiki_FR_Enrich1_20191106/Kontiki_FR_Enrich1_20191115.csv", "r", encoding="latin1") as file:
    all_row = file.readlines()
    file.close()

count = 0
list_md5 = []
list_check = []
for row in all_row:
    each_obj = row.strip().split(";")
    list_md5.append(each_obj[0])
    if each_obj[0] == '""':
        # print("line before ", all_row[count-1], "\n", " line ", count-2, row)
        # print(all_row[count - 1] + "\n" + row)
        list_check.append(row)
        # all_row.remove(row)
    count += 1

list_set_md5 = list(set(list_md5))

print(" Here is the number of row \'Kontiki_FR_Enrich1_20191106.csv\' --> ", len(all_row))
print(" length of list_md5 = ", len(list_md5), " -- len set_liste", len(list_set_md5))

count_distinct = Counter(list_md5)
for key, value in count_distinct.items():
    if value > 1:
        print(key, " répété {} fois".format(value))

with open("../../Downloads/DW/DATAS EXPORT/Kontiki_FR_Enrich1_20191106/Kontiki_FR_Enrich1_20191115_new.csv", "w", encoding="latin1") as f:
    lines = [i for i in all_row if i not in list_check]
    # f.seek(0)
    f.writelines(lines)
    # f.truncate()
    f.close()
    print("48 lines deleting successfull!")

print("execution time : %s secondes ---" % (time.time() - start_time))
