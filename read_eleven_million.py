import connect_pg
import dealing_csv


with open("../../Downloads/DW/DATAS EXPORT/Kontiki_FR_Enrich1_20191106/Kontiki_FR_Enrich1_20191106.csv", "r", encoding="utf-8") as file:
    all_row = file.readlines()
    file.close()


print(" Here is the number of row \'Kontiki_FR_Enrich1_20191106.csv\' --> ", len(all_row))
