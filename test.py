with open("../../Downloads/DW/DATAS EXPORT/Kontiki_FR_Enrich1_20191106/Kontiki_FR_Enrich1_20191115_new.csv", "r", encoding="latin1") as file:
    all_row = file.readlines()
    file.close()
print(len(all_row))
a = ""
for row in all_row:
    each_obj = row.strip().split(";")
    if each_obj[0] == '""':
        print("Trouver Trouver")
        a = "Oka le"

if a != "":
    print("Tsy milamina kely an!! ")
else:
    print("NORRRRRMMMMMMEEEEE ANNNANNNNNNNN!!!! ")