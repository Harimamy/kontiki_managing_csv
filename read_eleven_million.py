import glob
import numpy as np
import connect_pg
import dealing_csv
from collections import Counter
import time
import os
import zipfile
import connect_pg
import pandas as pd
from SoloLearn import SoloLearn
import json

pd.set_option('display.max_rows', 1000)
pd.set_option('display.max_columns', 1000)
pd.set_option('display.width', 2000)


class DealElevenMillions(object):
    @staticmethod
    def read_eleven_millions():
        # with open("../../Downloads/DW/DATAS EXPORT/CSV/Kontiki_FR_Enrich1_20191106.csv", "r", encoding="utf-8") as file:
        with open("../../Downloads/DW/DATAS EXPORT/Kontiki_FR_Enrich1_20191106/Kontiki_FR_Enrich1_20191115_new.csv", "r", encoding="latin1") as file:
            all_row = file.readlines()[1:]
            file.close()

        list_md5_11million = [row.strip().split(";")[0][1:-1].lower() for row in all_row]
        return list_md5_11million

    @staticmethod
    def count_md5_in_11millions(list_email_md5, all_row, list_check):
        # list_set_md5 = list(set(list_email_md5))
        # print(" Here is the number of mail_md5 \'Kontiki_FR_Enrich1_20191106.csv\' --> ", len(all_row))
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
    def get_all_md5_infile_with_index(path, index):
        data_md5_file = deal_obj.reading_csv(path)
        return [row.strip().split(",")[index] for row in data_md5_file]

    @staticmethod
    def get_all_md5_for_query(path):
        data_md5_file = deal_obj.reading_csv(path)
        return ["'" + str(row.strip().split(",")[1]) + "'," for row in data_md5_file]

    @staticmethod
    def get_all_md5_for_query_agenda_vp(path):
        data_md5_file = deal_obj.reading_csv(path)
        return ["'" + str(row.strip().split(",")[0]) + "'," for row in data_md5_file]

    @staticmethod
    def get_all_md5_per_file(path):
        data_md5_file = deal_obj.reading_csv(path)
        return [str(row.strip().split(",")[0]) for row in data_md5_file]

    @staticmethod  # valid for Club des Reducs "Base"
    def get_list_five_points(path, index_last_click, index_em_last_click):
        list_five_points, data_base_thematic = [], deal_obj.reading_csv(path)
        for row in data_base_thematic:
            list_data_split = row.strip().split(",")
            # if lastclickemail and em_lastclick == '' ## for 5 points
            if list_data_split[index_last_click] == '' and list_data_split[index_em_last_click] == '':
                list_five_points.append("'" + list_data_split[1] + "',")
        return list_five_points

    @staticmethod  # valid for Club des Reducs "Base"
    def get_list_ten_points(path, index_last_click, index_em_last_click):
        list_ten_points, data_base_thematic = [], deal_obj.reading_csv(path)
        for row in data_base_thematic:
            list_data_split = row.strip().split(",")
            # if lastclickemail and em_lastclick != '' ## for 10 points
            if list_data_split[index_last_click] != '' or list_data_split[index_em_last_click] != '':
                list_ten_points.append("'" + list_data_split[1] + "',")
        return list_ten_points

    @staticmethod
    def get_set_list_ten_points_per_base(path_content_csv, path_tuple_content_md5_click):
        tuple_ten_points = ()
        list_file_per_tag = glob.glob(path_content_csv + "/" + "*.csv")
        for file_md5_tag in list_file_per_tag:
            df_tag = pd.read_csv(file_md5_tag, low_memory=False)
            tuple_ten_points += tuple((row[1]['emailmd5'] for row in df_tag.iterrows() if pd.notna(row[1]['lastclickemail'])))
            print("len() tag >>> {} ==========>".format(file_md5_tag[60:]), len(tuple_ten_points))
        print(len(set(tuple_ten_points)))
        DealElevenMillions.output_infile_write_str_tuple(path_tuple_content_md5_click, tuple(set(tuple_ten_points)))
        return set(tuple_ten_points)

    @staticmethod
    def distinct_date_click_per_md5_for_number_click(file_content_md5_click, path_content_csv, output_path_file_content_number_score, output_path_distinct_date):
        dict_ten_point = {elt: [] for elt in DealElevenMillions.content_file_to_eval_list(file_content_md5_click)}
        for item in glob.glob(path_content_csv + "/" + "*.csv"):
            # recup all ten points md5
            try:
                for i in pd.read_csv(item, low_memory=False).iterrows():
                    if pd.notna(i[1]['lastclickemail']):
                        dict_ten_point[i[1]['emailmd5']].append(i[1]['lastclickemail'])
            except Exception:
                raise OSError("catch in by Harry exception file!!...")
            finally:
                print(item[:-10], " dict_ten_point =========================================>>", len(dict_ten_point))
                print("*" * 200, "\n")

        with open(output_path_file_content_number_score, "w") as file:
            file.write(json.dumps(SoloLearn.group_by_values({key: len(list(set(value))) for key, value in dict_ten_point.items()})))  # asio len aloha lay set value io rehefa anoratra score

        with open(output_path_distinct_date, "w") as file:
            file.write(json.dumps({key: list(set(value)) for key, value in dict_ten_point.items()}))  # asio len aloha lay set value io rehefa anoratra score

    @staticmethod  # valid for agendaVP "Base"
    def get_list_ten_points_agenda_vp(path, index_last_click):
        list_10_points, data_base_thematic = [], deal_obj.reading_csv(path)
        for row in data_base_thematic:
            list_data_split = row.strip().split(",")
            # if lastclickemail != '' ## for 10 points
            if list_data_split[index_last_click] != '':
                list_10_points.append("'" + list_data_split[0] + "',")
        return list_10_points

    @staticmethod
    def output_infile_write(path, list_to_write_on_file):
        with open(path, "w", encoding="latin1") as file_output:
            file_output.writelines(list_to_write_on_file)
            file_output.close()

    @staticmethod
    def output_infile_write_str_tuple(path, tuple_to_write_on_file):
        with open(path, "w", encoding="latin1") as file_output:
            file_output.write(str(tuple(tuple_to_write_on_file)))
            file_output.close()

    @staticmethod
    def tuple_content_difference(superior_tuple, inferior_tuple):
        return tuple(x for x in superior_tuple if x not in inferior_tuple)

    @staticmethod
    def unzip_all_file_method(folder, folder_to_extract):
        counter = 0
        for item in os.listdir(folder):
            if item.endswith(".zip"):
                with zipfile.ZipFile(folder + "/" + item, 'r') as zip_ref:
                    zip_ref.extractall(folder_to_extract)
                counter += 1
                zip_ref.close()
        print("{} file(s) unzip successfully!".format(counter))

    @staticmethod
    def generate_query():
        pass

    @staticmethod
    def unzip_and_generate_md5_per_tag(folder_contain_zip, folder_contain_csv, folder_to_contain_text_file, base_name):
        main_deal.unzip_all_file_method(folder_contain_zip, folder_contain_csv)
        list_all_md5_base = []
        for item in os.listdir(folder_contain_csv):
            path = os.path.join(folder_contain_csv, item)
            if os.path.isdir(path):
                pass
            elif item.endswith(".csv"):
                list_all_md5_per_file = list(pd.read_csv(path, low_memory=False)['emailmd5'])
                print(item, "======================>>", len(list_all_md5_per_file))
                main_deal.output_infile_write_str_tuple(folder_to_contain_text_file + "{}{}.txt".format("/", item[:-3]), list_all_md5_per_file)
                list_all_md5_base.extend(list_all_md5_per_file)
        main_deal.output_infile_write_str_tuple(folder_to_contain_text_file + "/all_md5_" + base_name + ".txt", set(list_all_md5_base))
        DealElevenMillions.write_all_column_name(folder_contain_csv)

    @staticmethod
    def recover_all_md5_not_in_11m(path_content_all_csv):
        list_all_one_base, list_file_csv = [], glob.glob(path_content_all_csv + "/" + "*.csv")
        [list_all_one_base.extend(list(pd.read_csv(file_csv, low_memory=False)['emailmd5'])) for file_csv in list_file_csv]
        return list(set(list_all_one_base))

    @staticmethod
    def write_all_column_name(path_content_all_csv):
        list_file_csv = glob.glob(path_content_all_csv + "/" + "*.csv")
        # column name display
        for file_csv in list_file_csv:
            df = pd.read_csv(file_csv, low_memory=False)
            print(df.columns, "===>", file_csv[10:-19])
            # main_deal.output_infile_write_str_list(file_csv[:63] + "md5 per tag/" + file_csv[63:-19] + ".txt", list(df['emailmd5']))

    @staticmethod
    def generate_tuple_md5_not_in_11million(check_value, path_content_file_all_md5_base, path_file_content_difference, last_id_md5):
        cnx, curs = connect_pg.postgres_connect()
        list_md5_base_checked = list(pd.read_sql_query('SELECT "MD5" FROM "DWH"."Kontiki_FR_11Mmd5" WHERE "Check"=' + str(check_value), con=cnx)['MD5'])  # type: Any
        print("all recover md5 in base OK >>>", len(list_md5_base_checked), "MD5 checked")
        remain_md5 = set(DealElevenMillions.content_file_to_eval_list(path_content_file_all_md5_base)) - set(list_md5_base_checked)
        print("the difference is >>> ", len(remain_md5))
        DealElevenMillions.output_infile_write_str_tuple(path_file_content_difference, remain_md5)
        print("write difference in file successfully!")
        a = input("enter c too continue and insert all difference between 11M! else choice another for quite!!.. \nthe id md5 is {}".format(str(last_id_md5)))
        if a == 'c':
            id_md5 = last_id_md5
            for mail_md5 in main_deal.content_file_to_eval_list(path_file_content_difference):
                try:
                    query_one = 'INSERT INTO "DWH"."Kontiki_FR_11Mmd5"("MD5") VALUES ({0}'.format("'") + mail_md5 + '{0});'.format("'")
                    query_two = 'INSERT INTO "DWH"."Main_DATAS"("id", "emailmd5") VALUES ({1} ,{0}'.format("'", id_md5) + mail_md5 + '{0});'.format("'")
                    print(query_one, query_two)
                    deal_obj.insert_to_pg(query=query_one)
                    deal_obj.insert_to_pg(query=query_two)
                except Exception:
                    raise ValueError("An error occured by Harry!!")
                finally:
                    # pass
                    print("successfull inserted in line " + str(id_md5) + "database!!")
                    id_md5 += 1
        else:
            #break
            pass

    @staticmethod
    def content_file_to_eval_list(path_file):
        return eval(open(path_file, "r").read())

    @staticmethod
    def verification_field(path_content_all_csv):
        list_file_csv = glob.glob(path_content_all_csv + "/" + "*.csv")
        for file_csv in list_file_csv:
            # [print(i,"in file", file_csv) for i in list(pd.read_csv(file_csv, low_memory=False)['insee_code']) if pd.notna(i)]
            [print(i,"in file", file_csv) for i in list(pd.read_csv(file_csv, low_memory=False)['browser']) if pd.notna(i)]
            # [print(i,"in file", file_csv) for i in list(pd.read_csv(file_csv, low_memory=False)['firstname']) if pd.notna(i)]
            # [print(i,"in file", file_csv) for i in list(pd.read_csv(file_csv, low_memory=False)['lastname']) if pd.notna(i)]
            # [print(i,"in file", file_csv) for i in list(pd.read_csv(file_csv, low_memory=False)['population_per_zipcode']) if pd.notna(i)]
            # [print(i,"in file", file_csv) for i in list(pd.read_csv(file_csv, low_memory=False)['density_per_zipcode']) if pd.notna(i)]
            # [print(i,"in file", file_csv) for i in list(pd.read_csv(file_csv, low_memory=False)['density_score']) if pd.notna(i)]
            # [print(i,"in file", file_csv) for i in list(pd.read_csv(file_csv, low_memory=False)['commune']) if pd.notna(i)]

    @staticmethod
    def deduplication_four_million_md5_vs_blacklist():
        set_four_million_md5 = set(pd.read_csv("../../Downloads/DW/EXPORT pour Pierre/20200212_md5email_notfound.csv")['md5email'])
        # df = pd.read_excel("../../Downloads/DW/EXPORT pour Pierre/Blacklist MD5/BL_FR0_MD5seuls.xlsx", sheet_name='Feuil1')
        set_blacklist_fr0 = set(pd.read_excel("../../Downloads/DW/EXPORT pour Pierre/Blacklist MD5/BL_FR0_MD5seuls.xlsx", sheet_name='Feuil1')['mailmd5'])
        set_blacklist_fr1 = set(pd.read_excel("../../Downloads/DW/EXPORT pour Pierre/Blacklist MD5/BL_FR1_MD5seuls.xlsx", sheet_name='Feuil1')['mailmd5'])
        set_blacklist_fr2 = set(pd.read_csv("../../Downloads/DW/EXPORT pour Pierre/Blacklist MD5/BL_FR2_MD5seuls.csv", low_memory=False)['mailmd5'])
        set_blacklist_fr3 = set(pd.read_csv("../../Downloads/DW/EXPORT pour Pierre/Blacklist MD5/BL_FR3_MD5new.csv", low_memory=False)['mailmd5'])
        set_blacklist_fr4 = set(pd.read_excel("../../Downloads/DW/EXPORT pour Pierre/Blacklist MD5/BL_FR_4_MD5seuls.xlsx", sheet_name='Feuil1')['mailmd5'])

        """ for listing md5 per file using tolist() pandas 
        # print(len(pd.read_excel("../../Downloads/DW/EXPORT pour Pierre/Blacklist MD5/BL_FR0_MD5seuls.xlsx", sheet_name='Feuil1')['mailmd5'].tolist()))
        # print(len(pd.read_excel("../../Downloads/DW/EXPORT pour Pierre/Blacklist MD5/BL_FR1_MD5seuls.xlsx", sheet_name='Feuil1')['mailmd5'].tolist()))
        # print(len(pd.read_csv("../../Downloads/DW/EXPORT pour Pierre/Blacklist MD5/BL_FR2_MD5seuls.csv", low_memory=False)['mailmd5'].tolist()))
        # print(len(pd.read_csv("../../Downloads/DW/EXPORT pour Pierre/Blacklist MD5/BL_FR3_MD5new.csv", low_memory=False)['mailmd5'].tolist()))
        # print(len(pd.read_excel("../../Downloads/DW/EXPORT pour Pierre/Blacklist MD5/BL_FR_4_MD5seuls.xlsx", sheet_name='Feuil1')['mailmd5'].tolist())) """

        print("len BL0 >>>>> ", len(set_blacklist_fr0))
        print("len BL1 >>>>> ", len(set_blacklist_fr1))
        print("len BL2 >>>>> ", len(set_blacklist_fr2))
        print("len BL3 >>>>> ", len(set_blacklist_fr3))
        print("len BL4 >>>>> ", len(set_blacklist_fr4))
        answer_blacklist = set_blacklist_fr0 | set_blacklist_fr1 | set_blacklist_fr2 | set_blacklist_fr3 | set_blacklist_fr4
        DealElevenMillions.output_infile_write_str_tuple("../../Downloads/DW/EXPORT pour Pierre/Blacklist MD5/tuple_set_all_blacklist.txt", answer_blacklist)
        print("length of all blacklist ======>> ", len(answer_blacklist))
        # the_result_is = set_four_million_md5 - answer_blacklist
        # print(len(set_blacklist_fr0 & set_blacklist_fr1))
        # print(len(set_four_million_md5))


if __name__ == '__main__':
    start_time = time.time()
    main_deal = DealElevenMillions()
    deal_obj = dealing_csv.DealCsv()
    # DealElevenMillions.verification_field("../../Downloads/DW/DATAS EXPORT/DATABASE/Enviesdebonsplans/file_CSV")
    # DealElevenMillions.deduplication_four_million_md5_vs_blacklist()
    """print("Here is the list of remain file content md5! >>>>")"""
    # for item in glob.glob("../../Downloads/DW/DATAS EXPORT/DATABASE/LagendadesVP/file_CSV/md5 per tag/*.txt"):
    #     list_agenda.extend(main_deal.content_file_to_eval_list(item))
    #     print(item[:-5], "=====================>>> OK", len(main_deal.content_file_to_eval_list(item)))
    # print("here is the obtain by sum file >>>", len(set(tuple_md5_agenda_file)))
    # DealElevenMillions.output_infile_write_str_tuple("../../Downloads/DW/DATAS EXPORT/DATABASE/LagendadesVP/file_CSV/md5 per tag/All_LAGENDA_VP2.txt", set(list_agenda))

    """print("Here is the list of file md5 dealing! >>>>")"""
    # for item in glob.glob("../../Downloads/DW/DATAS EXPORT/DATABASE/Ma destinee/file txt/file dealing/*.txt"):
    #     list_md5_madestinee_deal.extend(main_deal.content_file_to_eval_list(item))
    #     print(item[62:], "=====================>>> OK", len(main_deal.content_file_to_eval_list(item)))
    # print("here is the obtain by sum file >>>", len(list(set(list_md5_madestinee_deal))))
    #
    # set_subtract_between_two_list = set(tuple_md5_agenda_file) - set(list_md5_madestinee_deal)
    # print(len(set_subtract_between_two_list))
    # main_deal.output_infile_write_str_tuple("../../Downloads/DW/DATAS EXPORT/DATABASE/Ma destinee/file txt/file dealing/other md5/all_md5_madestinee_remain.txt", set_subtract_between_two_list)

    # main_deal.output_infile_write_str_tuple("../../Downloads/DW/DATAS EXPORT/DATABASE/Ma destinee/file txt/all_md5_madestinee_remain.txt", tuple_md5_agenda_file)

    # DealElevenMillions.distinct_date_click_per_md5_for_number_click("../../Downloads/DW/DATAS EXPORT/DATABASE/LagendadesVP/file_CSV/tuple_md5_who_s_click.txt", "../../Downloads/DW/DATAS EXPORT/DATABASE/LagendadesVP/file_CSV",
    #                                                                 "../../Downloads/DW/DATAS EXPORT/DATABASE/LagendadesVP/file_CSV/output_number_click_by_SoloLearn_2.txt", "../../Downloads/DW/DATAS "
    #                                                                                                                                                                          "EXPORT/DATABASE/LagendadesVP/file_CSV/output_number_click_key_md5.txt")
    # DealElevenMillions.get_set_list_ten_points_per_base("../../Downloads/DW/DATAS EXPORT/DATABASE/LagendadesVP/file_CSV", "../../Downloads/DW/DATAS EXPORT/DATABASE/LagendadesVP/file_CSV/tuple_md5_who_s_click.txt")
    # main_deal.write_all_column_name("../../Downloads/DW/DATAS EXPORT/DATABASE/LagendadesVP/file_CSV/")
    # list_md5_base = main_deal.recover_all_md5_not_in_11m("../../Downloads/DW/DATAS EXPORT/DATABASE/LagendadesVP/file_CSV/")
    '''tuple_ma_destinee_not_in_base = main_deal.tuple_content_difference(main_deal.content_file_to_eval_list("../../Downloads/DW/DATAS EXPORT/DATABASE/Ma destinee/file txt/all_md5_madestinee.txt"),
                                                                 main_deal.content_file_to_eval_list("../../Downloads/DW/DATAS EXPORT/DATABASE/Ma destinee/file txt/md5_madestinee_checked_base.txt"))
    main_deal.output_infile_write_str_tuple("../../Downloads/DW/DATAS EXPORT/DATABASE/Ma destinee/file txt/md5_madestinee_not_in_base_11M.txt", tuple_ma_destinee_not_in_base)
    print(len(tuple_ma_destinee_not_in_base))'''

    """ first dealing about recovery all md5 """
    # liste_ink_travaux_madestinee = main_deal.content_file_to_eval_list("../../Downloads/DW/DATAS EXPORT/DATABASE/Ma destinee/file txt/Madestinee_TRAVAUX.txt")
    # liste_ink_travaux_madestinee.extend(main_deal.content_file_to_eval_list("../../Downloads/DW/DATAS EXPORT/DATABASE/Ma destinee/file txt/Madestinee_INK.txt"))
    # print(len(liste_ink_travaux_madestinee))
    # liste_ink_travaux_madestinee = tuple(set(liste_ink_travaux_madestinee))
    # print("after set fonction, we have only", len(liste_ink_travaux_madestinee))
    # main_deal.output_infile_write_str_tuple("../../Downloads/DW/DATAS EXPORT/DATABASE/Ma destinee/file txt/Ink_TRAVAUX.txt", liste_ink_travaux_madestinee)

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

    ''' md5 in database per thematic '''

    # main_deal.unzip_all_file_method("../../Downloads/DW/DATAS EXPORT/DATABASE/Clubdesreducs", "../../Downloads/DW/DATAS EXPORT/DATABASE/Clubdesreducs/File_CSV")

    # list_md5_11millions = main_deal.read_eleven_millions()
    # path = "../../Downloads/DW/DATAS EXPORT/DATABASE/Clubdesreducs/Difference"
    # list_md5_not_in_db = []
    # for item in os.listdir(path):
    #     # list_md5_difference = main_deal.get_all_md5_infile(path + "/" + item)
    #     # print(item[14:-10], "=========================================>>", len(list(set(list_md5_by_tag))))
    #     with open(path + "/" + item, "r") as file:
    #         all_row = file.readlines()
    #         list_md5_not_in_db += all_row
    #         print(all_row[:3])
    #         file.close()
    # print(len(list(set(list_md5_not_in_db))))

    # liste_cmc_voyance = DealElevenMillions.get_all_md5_per_file("../../Downloads/DW/DATAS EXPORT/DATABASE/Consommer moins cher/file_CSV/consommermoinscher_VOYANCE_2020_02_11.csv")
    # DealElevenMillions.output_infile_write_str_tuple("../../Downloads/DW/DATAS EXPORT/DATABASE/Consommer moins cher/file txt/consommermoinscher_VOYANCE_2020_02_11.txt", liste_cmc_voyance)
    """FIRST STEP after receiving all zip file for just One BASE"""
    main_deal.unzip_and_generate_md5_per_tag("../../Downloads/DW/DATAS EXPORT/DATABASE/Le bon kdo", "../../Downloads/DW/DATAS EXPORT/DATABASE/Le bon kdo/file_CSV", "../../Downloads/DW/DATAS EXPORT/DATABASE/Le bon kdo/file txt",
                                             "Le bon kdo")

    '''connect to pgsql for recuper all md5 checked'''
    DealElevenMillions.generate_tuple_md5_not_in_11million(97, "../../Downloads/DW/DATAS EXPORT/DATABASE/Le bon kdo/file txt/all_md5_Le bon kdo.txt", "../../Downloads/DW/DATAS EXPORT/DATABASE/Le bon kdo/file "
                                                                                                                                                                   "txt/file_content_difference_Le bon kdo_11m.txt", 11130542)

    #
    # # time2 = time.time()
    # list_md5_agenda_vp_defisc = list(pd.read_csv("../../Downloads/DW/DATAS EXPORT/DATABASE/LagendadesVP/file_CSV/LAGENDA_DECORATION_2020_01_09_18_34_57.csv", low_memory=False)['emailmd5'])
    # print(list_md5_agenda_vp_decoration[:5], len(list_md5_agenda_vp_decoration))
    # list_all_garden = main_deal.content_file_to_eval_list("../../Downloads/DW/DATAS EXPORT/DATABASE/Ma destinee/file txt/dealing GARDEN/Madestinee_GARDEN_part_2.txt")
    # list_checked_garden = main_deal.content_file_to_eval_list("../../Downloads/DW/DATAS EXPORT/DATABASE/Ma destinee/file txt/dealing GARDEN/md5_madestinee_checked_garden.txt")
    # main_deal.output_infile_write_str_tuple("../../Downloads/DW/DATAS EXPORT/DATABASE/Ma destinee/file txt/dealing GARDEN/Madestinee_GARDEN_part_1.txt", list_all_garden[:10000])
    # main_deal.output_infile_write_str_tuple("../../Downloads/DW/DATAS EXPORT/DATABASE/Ma destinee/file txt/dealing GARDEN/Madestinee_GARDEN_part_4.txt", list_all_garden[:2000])
    # print(len(list_all_garden), len(list_checked_garden))

    ''' here to generate the difference after collect md5 checked in base '''
    # tuple_diff = main_deal.tuple_content_difference(list_all_garden, list_checked_garden)
    # print("Here is the number of difference", len(tuple_diff), len(list(set(tuple_diff))))
    # main_deal.output_infile_write_str_tuple("../../Downloads/DW/DATAS EXPORT/DATABASE/Ma destinee/file txt/dealing GARDEN/md5_garden_not_in 11M.txt", tuple_diff)

    ######### id_md5 = 11086849
    # id_md5 = 11110105
    # for mail_md5 in main_deal.content_file_to_eval_list("../../Downloads/DW/DATAS EXPORT/DATABASE/Consommer moins cher/file txt/another txt/md5_cmc_difference.txt"):
    #     try:
    #         query_one = 'INSERT INTO "DWH"."Kontiki_FR_11Mmd5"("MD5") VALUES ({0}'.format("'") + mail_md5 + '{0});'.format("'")
    #         query_two = 'INSERT INTO "DWH"."Main_DATAS"("id", "emailmd5") VALUES ({1} ,{0}'.format("'", id_md5) + mail_md5 + '{0});'.format("'")
    #         print(query_one, query_two)
    #         deal_obj.insert_to_pg(query=query_one)
    #         deal_obj.insert_to_pg(query=query_two)
    #     except Exception:
    #         raise ValueError("An error occured by Harry!!")
    #     finally:
    #         print("successfull inserted in line " + str(id_md5) + "database!!")
    #         id_md5 += 1
    ########## print("=========================================================================>>", len(all_row), "rows md5", item[12:-4], "successfull inserting!")


    # # list_intersect_beauty_cr_lgvp = [elt for elt in list_md5_clubreducs_cars if elt in list_md5_beauty_me]
    # # print(len(list_intersect_beauty_cr_lgvp), list_intersect_beauty_cr_lgvp[:4])
    # # with open("../../Downloads/DW/DATAS EXPORT/DATABASE/LagendadesVP/file txt/Ancien pratique/verif_all_point_5.txt", "w") as wr:
    #     wr.write(str(list_intersect_beauty_cr_lgvp))

    # list_md5_cosmetics_file_agenda_vp = main_deal.get_all_md5_infile_with_index(path='../../Downloads/DW/DATAS EXPORT/DATABASE/LagendadesVP/file_CSV/LAGENDA_COSMETICS_2020_01_09_18_45_27.csv', index=0)
    # main_deal.output_infile_write_str_list("..\..\Dpwnloads\DW\DATAS EXPORT\DATABASE\LagendadesVP/file_CSV\edit file/str_all_cosmetics_agenda_VP.txt", tuple_to_write_on_file=list_md5_cosmetics_file_agenda_vp)
    # main_deal.unzip_all_file_method(folder="../../Downloads/DW/DATAS EXPORT/DATABASE/Ma destinee", folder_to_extract="../../Downloads/DW/DATAS EXPORT/DATABASE/Ma destinee/file_CSV")
    # main_deal.output_infile_write_str_tuple('../../Downloads/DW/TUPLE_BL_Domains_seuls_FR_2018_09.txt', tuple(open('../../Downloads/DW/BL_Domains_seuls_FR_2018_09.csv', 'r').read().splitlines()))
    path = "../../Downloads/DW/DATAS EXPORT/DATABASE/Le bon kdo/file txt/compter"
    len_lbk = sum([len(DealElevenMillions.content_file_to_eval_list(path + "/" + file_lbk)) for file_lbk in os.listdir(path)])
    print(len_lbk)

    print("*" * 200)
    print("execution time : %s secondes ---" % (time.time() - start_time))


