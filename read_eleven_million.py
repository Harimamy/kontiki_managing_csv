import connect_pg
import dealing_csv
from collections import Counter
import time


class DealElevenMillions(object):
    @staticmethod
    def read_eleven_millions():
        # with open("../../Downloads/DW/DATAS EXPORT/CSV/Kontiki_FR_Enrich1_20191106.csv", "r", encoding="utf-8") as file:
        with open("../../Downloads/DW/DATAS EXPORT/Kontiki_FR_Enrich1_20191106/Kontiki_FR_Enrich1_20191115_new.csv", "r", encoding="latin1") as file:
            all_row = file.readlines()[1:]
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
                    print("Eeeeeh! take care please! you have some error on this file... ")
                    list_check.append(row)
                    # all_row.remove(row)
                count += 1
        return list_md5

    def count_md5(self, list_email_md5, all_row, list_check):
        # list_set_md5 = list(set(list_email_md5))
        # print(" Here is the number of row \'Kontiki_FR_Enrich1_20191106.csv\' --> ", len(all_row))
        # print(" length of list_md5 = ", len(list_email_md5), " -- len set_liste", len(list_set_md5))

        count_distinct = Counter(list_email_md5)
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

    def get_list_five_points(self, path, index_last_click, index_em_last_click):
        list_five_points = []
        data_base_thematic = deal_obj.reading_csv(path)
        for row in data_base_thematic:
            list_data_split = row.strip().split(",")
            # if lastclickemail and em_lastclick == '' ## for 5 points
            if list_data_split[index_last_click] and list_data_split[index_em_last_click] == '':
                list_five_points.append("'" + list_data_split[1] + "',")
        return list_five_points

    def output_infile_write(self, path, list_to_write_on_file):
        with open(path, "w", encoding="latin1") as file_output:
            file_output.writelines(list_to_write_on_file)
            file_output.close()


if __name__ == '__main__':
    start_time = time.time()
    main_deal = DealElevenMillions()
    deal_obj = dealing_csv.DealCsv()
    # list_eleven_md5 = main_deal.read_eleven_millions()

    # list_eleven_md5_rectif = []
    # list_sampn = []
    # for elt in list_eleven_md5:
    #     if len(elt) < 32:
    #         print(elt," at line ", list_eleven_md5.index(elt))
    #         list_sampn.append(elt)
    #     else:
    #         elt = elt[1:-1].lower()
    #         list_eleven_md5_rectif.append(elt)

    list_md5_exist_in_base = deal_obj.get_to_pg('SELECT "MD5" FROM "DWH"."Kontiki_FR_11Mmd5" WHERE "Base" = \'Club des Reducs\'')
    print("here is the number of md5 on database: ", len(list_md5_exist_in_base), "the type is", type(list_md5_exist_in_base[1]))
    list_md5_base = [element[0] for element in list_md5_exist_in_base]
    print(list_md5_base[:5])
    print("number of liste source database is:", len(list_md5_base))

    list_md5_cr_car = []
    total_md5_tocompare = []
    list_5_points = []
    data_clubdesreducs_cars = deal_obj.reading_csv("..\..\Downloads\DW\DATAS EXPORT\DATABASE\Clubdesreducs\Clubdesreducs_CARS_2019_11_29.csv")
    for row in data_clubdesreducs_cars:
        list_data = row.strip().split(",")
        # list_md5_cr_car.append("'" + list_data[1] + "',")
        total_md5_tocompare.append(list_data[1])
        # if list_data[14] == '' and list_data[23] == '':
        #     list_5_points.append("'" + list_data[1] + "',")
        # normally we have: 51687 len(list_md5_cr_car)

    print(len(total_md5_tocompare), total_md5_tocompare[:5])
    list_in_cr_car_notbase = [x for x in total_md5_tocompare if x not in list_md5_base]
    print("here is the number of difference: ", len(list_in_cr_car_notbase))

    with open("../../Downloads/DW/DATAS EXPORT/Kontiki_FR_Enrich1_20191106/out_not_inside_md5_in_cr_car.txt", "w", encoding="latin1") as file_output:
        for i in list_in_cr_car_notbase:
            file_output.write(i + "\n")
        file_output.close()

    # print("we have the number of email md5 in club des reducs CAR :", len(list_md5_cr_car), "and the eleven million :", len(list_eleven_md5))
    # print("here is the number of 5 points: ", len(list_5_points))
    print("*"*200)
    # print("all email md5 crypted in md5_cr_CAR: ", len(list_md5_cr_car))
    print("*"*200)

    print("execution time : %s secondes ---" % (time.time() - start_time))
