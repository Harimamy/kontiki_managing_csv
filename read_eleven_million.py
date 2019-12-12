import connect_pg
import dealing_csv
from collections import Counter
import time

start_time = time.time()

# with open("../../Downloads/DW/DATAS EXPORT/CSV/Kontiki_FR_Enrich1_20191106.csv", "r", encoding="utf-8") as file:

with open("../../Downloads/DW/DATAS EXPORT/CSV/Kontiki_FR_Enrich1_20191115.csv", "r", encoding="latin1") as file:
    all_row = file.readlines()
    file.close()

count = 0
list_md5 = []
for row in all_row[1:]:
    count += 1
    each_obj = row.strip().split(";")
    list_md5.append(each_obj[0])
    if len(each_obj[0]) < 2:
        print("True and find at line ", count)

list_set_md5 = list(set(list_md5))

print(" Here is the number of row \'Kontiki_FR_Enrich1_20191106.csv\' --> ", len(all_row))
print(" length of list_md5 = ", len(list_md5), " -- len set_liste", len(list_set_md5))

count_distinct = Counter(list_md5)
for key, value in count_distinct.items():
    if value > 1:
        print(key, " répété {} fois".format(value))


print("execution time : %s secondes ---" % (time.time() - start_time))
