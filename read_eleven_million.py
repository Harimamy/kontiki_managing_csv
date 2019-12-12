import connect_pg
import dealing_csv
import time

start_time = time.time()

with open("../../Downloads/DW/DATAS EXPORT/Kontiki_FR_Enrich1_20191106/Kontiki_FR_Enrich1_20191106.csv", "r", encoding="utf-8") as file:
    all_row = file.readlines()
    file.close()

list_md5 = []
for row in all_row:
    each_obj = row.strip().split(";")
    list_md5.append(each_obj[0])

list_set_md5 = list(set(list_md5))

print(" Here is the number of row \'Kontiki_FR_Enrich1_20191106.csv\' --> ", len(all_row))
print(" len de la liste list_md5 = ", len(list_md5), " -- len set_liste", len(list_set_md5))

print("execution time : %s secondes ---" % (time.time() - start_time))
