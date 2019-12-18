import connect_pg
import dealing_csv
from collections import Counter
import time
import os
import zipfile


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

    def count_md5_in_11millions(self, list_email_md5, all_row, list_check):
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

    @staticmethod
    def get_all_md5_infile(path):
        data_md5_file = deal_obj.reading_csv(path)
        return [row.strip().split(",")[1] for row in data_md5_file]

    @staticmethod
    def get_all_md5_for_query(path):
        data_md5_file = deal_obj.reading_csv(path)
        return ["'" + str(row.strip().split(",")[1]) + "'," for row in data_md5_file]

    @staticmethod
    def get_list_five_points(path, index_last_click, index_em_last_click):
        list_five_points, data_base_thematic = [], deal_obj.reading_csv(path)
        for row in data_base_thematic:
            list_data_split = row.strip().split(",")
            # if lastclickemail and em_lastclick == '' ## for 5 points
            if list_data_split[index_last_click] == '' and list_data_split[index_em_last_click] == '':
                list_five_points.append("'" + list_data_split[1] + "',")
        return list_five_points

    @staticmethod
    def get_list_ten_points(path, index_last_click, index_em_last_click):
        list_ten_points, data_base_thematic = [], deal_obj.reading_csv(path)
        for row in data_base_thematic:
            list_data_split = row.strip().split(",")
            # if lastclickemail and em_lastclick != '' ## for 10 points
            if list_data_split[index_last_click] != '' or list_data_split[index_em_last_click] != '':
                list_ten_points.append("'" + list_data_split[1] + "',")
        return list_ten_points

    @staticmethod
    def output_infile_write(path, list_to_write_on_file):
        with open(path, "w", encoding="latin1") as file_output:
            file_output.writelines(list_to_write_on_file)
            file_output.close()

    @staticmethod
    def list_difference(superior_list, inferior_list):
        return [x for x in superior_list if x not in inferior_list]

    @staticmethod
    def unzip_all_file_method(folder, folder_to_extract):
        counter = 0
        for item in os.listdir(folder):
            if item.endswith(".zip"):
                with zipfile.ZipFile(folder + "/" + item, 'r') as zip_ref:
                    zip_ref.extractall(folder_to_extract)
                counter += 1
                zip_ref.close()
        print("{} file unzip successfully!".format(counter))


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

    # list_md5_tag_car = deal_obj.get_to_pg('SELECT "MD5" FROM "DWH"."Kontiki_FR_11Mmd5" WHERE "Cars" = 5')
    # print("here is the number of md5 on Cars: ", len(list_md5_tag_car))
    # list_md5_tag_car_indb = [element[0] for element in list_md5_tag_car]
    # print(list_md5_tag_car_indb[:5])

    list_md5_cr_car = []
    list_md5_cr_cosmetics = []
    list_5_points = []
    # data_clubdesreducs_cars = deal_obj.reading_csv("..\..\Downloads\DW\DATAS EXPORT\DATABASE\Clubdesreducs\Clubdesreducs_CARS_2019_11_29.csv")
    # for row in data_clubdesreducs_cars:
    #     list_data = row.strip().split(",")
    #     list_md5_cr_car.append(list_data[1])
    # normally we have: 51687 len(list_md5_cr_car)

    list_md5_cosmetic = main_deal.get_all_md5_for_query("..\..\Downloads\DW\DATAS EXPORT\DATABASE\Clubdesreducs\Clubdesreducs_COMESTICS_2019_11_29_11_11_39.csv")
    # print("=============================================>>> ", len(list_md5_cosmetic))
    print(list_md5_cosmetic[:7])
    # list_intersect_cars_cosmetics = main_deal.list_difference(list_md5_cr_car, list_md5_cr_cosmetics)
    # list_five_points_cosmetics = main_deal.get_list_five_points("..\..\Downloads\DW\DATAS EXPORT\DATABASE\Clubdesreducs\Clubdesreducs_COMESTICS_2019_11_29_11_11_39.csv", 14, 23)
    # list_ten_points_cosmetics = main_deal.get_list_ten_points("..\..\Downloads\DW\DATAS EXPORT\DATABASE\Clubdesreducs\Clubdesreducs_COMESTICS_2019_11_29_11_11_39.csv", 14, 23)

    # list_diff_car5pt = main_deal.list_difference(list_5_points, list_md5_tag_car_indb)
    # print("here is the number of difference: ", len(list_diff_car5pt))

    main_deal.unzip_all_file_method("../../Downloads/DW/DATAS EXPORT/DATABASE/Clubdesreducs", "../../Downloads/DW/DATAS EXPORT/DATABASE/Clubdesreducs/File_CSV")

    # main_deal.output_infile_write("..\..\Downloads\DW\DATAS EXPORT\DATABASE\Clubdesreducs\File txt\out_CR_cosmetics_5points.txt", list_five_points_cosmetics)
    # main_deal.output_infile_write("..\..\Downloads\DW\DATAS EXPORT\DATABASE\Clubdesreducs\File txt\out_CR_cosmetics_10points.txt", list_ten_points_cosmetics)

    # print("we have the number of email md5 in club des reducs CAR :", len(list_md5_cr_car), "and the eleven million :", len(list_eleven_md5))
    print("*" * 200)
    print("*" * 200)

    print("execution time : %s secondes ---" % (time.time() - start_time))
