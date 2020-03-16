import numpy as np
import pandas as pd
import os
import dealing_csv
import read_eleven_million
import time
import connect_pg
import psycopg2
import json
import glob
from SoloLearn import SoloLearn
from typing import Any
from sqlalchemy import create_engine


def first_dealing(path_cv, query, path_out_dict_json):
    deal_obj = dealing_csv.DealCsv()
    import matplotlib.pyplot as plt

    # first_test = pd.read_csv("../../Downloads/DW/DATAS EXPORT/Kontiki_FR_Enrich1_20191106/Kontiki_FR_Enrich1_20191115_new.csv", encoding="latin1")
    # print(first_test.describe())
    # print(df_csv.shape, df_csv)
    # print(first_test['score_pauvrete'].value_counts().plot.bar())
    # print(first_test['score_mediane'].hist())
    # start_time = time.time()
    # list_md5_clubreducs = [element[0] for element in deal_obj.get_to_pg('SELECT "MD5" FROM "DWH"."Kontiki_FR_11Mmd5" WHERE "Base" = \'Club des Reducs\'')]
    # print(list_md5_clubreducs[:5])
    # print("execution time for psycopg2 --- : %s secondes ---" % (time.time() - start_time))
    #
    inter_time = time.time()
    cnx, curs = connect_pg.postgres_connect()
    list_md5_clubreducs_click = list(pd.read_sql_query('SELECT "MD5" FROM "DWH"."Kontiki_FR_11Mmd5" WHERE "score_club_des_Reducs" IS NOT NULL', con=cnx)['MD5'])  # type: Any
    print(list_md5_clubreducs_click[:5], len(list_md5_clubreducs_click))
    print("execution time for pandas sql query--- : %s secondes ---" % (time.time() - inter_time))
    dict_ten_point = {elt: [] for elt in list_md5_clubreducs_click}

    path = "../../Downloads/DW/DATAS EXPORT/DATABASE/Clubdesreducs/File_CSV"
    for item in os.listdir(path):
        # recup all ten points md5
        try:
            couple_md5_last_click = [(i[1][1], i[1][14], i[1][23]) for i in pd.read_csv(path + "/" + item, low_memory=False).iterrows() if pd.notna(i[1][23]) or pd.notna(i[1][14])]
            print(item[14:-10], "=========================================>>", len(couple_md5_last_click))
            for element in couple_md5_last_click:
                if pd.notna(element[1]) and pd.notna(element[2]):
                    dict_ten_point[element[0]].extend([element[1], element[2]])
                elif pd.notna(element[1]) or pd.notna(element[2]):
                    dict_ten_point[element[0]].append(element[1]) if pd.notna(element[1]) else dict_ten_point[element[0]].append(element[2])
        except Exception:
            raise OSError("catch in by Harry exception file!!...")
        finally:
            print(item[14:-10], " dict_ten_point =========================================>>", len(dict_ten_point))
            print("*" * 200, "\n")

    # with open("../../Downloads/DW/DATAS EXPORT/DATABASE/Clubdesreducs/File txt/10 points/dict_json_dump.txt", "w") as file:
    #     file.write(json.dumps(dict_ten_point)) # use `json.loads` to do the reverse


    # with open("../../Downloads/DW/DATAS EXPORT/DATABASE/Clubdesreducs/File txt/10 points/dict_json_dump_set.txt", "w") as file:
    #     file.write(json.dumps({key: list(set(value)) for key, value in dict_ten_point.items()})) # asio len aloha lay set value io rehefa anoratra score

    with open("../../Downloads/DW/DATAS EXPORT/DATABASE/Clubdesreducs/File txt/10 points/dict_json_sololearn_num.txt", "w") as file:
        file.write(json.dumps(SoloLearn.group_by_values({key: len(list(set(value))) for key, value in dict_ten_point.items()})))  # asio len aloha lay set value io rehefa anoratra score

    # df_csv = pd.read_csv("../..\Downloads\DW\DATAS EXPORT\DATABASE\Clubdesreducs\File_CSV\Clubdesreducs_CARS_2019_11_29.csv")


def verification_of_repetition_data(path_csv, path_out):
    connexion, cursor = connect_pg.postgres_connect()
    list_md5_clubdesreducs_in_base = list(pd.read_sql_query('SELECT "MD5" FROM "DWH"."Kontiki_FR_11Mmd5" WHERE "Base" LIKE \'Club%\'', con=connexion)['MD5'])

    dict_md5_cr_density_km2_individual_house = {element: [] for element in list_md5_clubdesreducs_in_base}
    dict_md5_cr_property_menage_impose = {element: [] for element in list_md5_clubdesreducs_in_base}
    dict_md5_cr_poverty_median = {element: [] for element in list_md5_clubdesreducs_in_base}
    print(list_md5_clubdesreducs_in_base[:7], len(list_md5_clubdesreducs_in_base))

    # Unicity verification per column
    for item in os.listdir(path_csv):
        try:
            tuple_md5_other_column_7 = [dict_md5_cr_density_km2_individual_house[i[1][1]].extend([i[1][19], i[1][20]]) for i in pd.read_csv(path_csv + "/" + item, low_memory=False).iterrows()]
            tuple_md5_other_column_8 = [dict_md5_cr_property_menage_impose[i[1][1]].extend([i[1][29], i[1][32]]) for i in pd.read_csv(path_csv + "/" + item, low_memory=False).iterrows()]
            tuple_md5_other_column_9 = [dict_md5_cr_poverty_median[i[1][1]].extend([i[1][33], i[1][34]]) for i in pd.read_csv(path_csv + "/" + item, low_memory=False).iterrows()]
            print(item[14:-10], "=========================================>> number of MD5: ", len(tuple_md5_other_column_7))
            print(item[14:-10], "=========================================>> number of MD5: ", len(tuple_md5_other_column_8))
            print(item[14:-10], "=========================================>> number of MD5: ", len(tuple_md5_other_column_9))
        except Exception:
            raise OSError("catch in by Harry exception file!!...")
        finally:
            print("*" * 200, "\n")
    with open("../../Downloads/DW/DATAS EXPORT/DATABASE/Clubdesreducs/edit file/dict_json_density_km2_individual_house.json", "w") as file:
        file.write(json.dumps({key: list(set(value)) for key, value in dict_md5_cr_density_km2_individual_house.items()}))  # asio len aloha lay set value io rehefa anoratra score
    with open("../../Downloads/DW/DATAS EXPORT/DATABASE/Clubdesreducs/edit file/dict_json_property_menage_impose.json", "w") as file:
        file.write(json.dumps({key: list(set(value)) for key, value in dict_md5_cr_property_menage_impose.items()}))  # asio len aloha lay set value io rehefa anoratra score
    with open("../../Downloads/DW/DATAS EXPORT/DATABASE/Clubdesreducs/edit file/dict_json_poverty_median.json", "w") as file:
        file.write(json.dumps({key: list(set(value)) for key, value in dict_md5_cr_poverty_median.items()}))  # asio len aloha lay set value io rehefa anoratra score


def insert_postgres_using_data_science(path_cv):
    for element in os.listdir(path=path_cv):
        df = pd.read_csv(os.path.join(path_cv, element), encoding="latin1")
        # pd.read_sql_query(query, con=connect_pg.postgres_connect()[0])


def test_for_one():
    df = pd.read_csv("..\..\Downloads\DW\DATAS EXPORT\DATABASE\Clubdesreducs\File_CSV\Clubdesreducs_ALARM_2019_11_29 - Copie.csv")
    df_main_data = df.drop(
        ['id', 'customsubscriberid', 'ip', 'vendor', 'trackingcode', 'geocountry', 'geostate', 'geocity', 'geozipcode', 'lastactivity', 'lastemail', 'lastopenemail', 'lastclickemail', 'density_per_km2', 'score_individual_houses',
         'em_creation', 'em_lastactivity', 'em_lastclick', 'em_lastmsg', 'em_lastopen', 'em_score', 'ip.1', 'num_msg', 'score_proprietaires', 'reoptindate', 'reoptinstatus', 'score_menages_imposes', 'score_pauvrete', 'score_mediane',
         'source_url', 'timestamp', 'vendor.1', 'zipcode'], axis=1)
    df_complements = df.drop(
        ['id', 'emailmd5', 'emailsha256', 'customsubscriberid', 'trackingcode', 'lastactivity', 'lastemail', 'lastopenemail', 'lastclickemail', 'subscriptiondate', 'birthdate', 'civility', 'country', 'density_per_km2',
         'score_individual_houses', 'em_creation', 'em_lastactivity', 'em_lastclick', 'em_lastmsg', 'em_lastopen', 'em_score', 'score_proprietaires', 'score_menages_imposes', 'score_pauvrete', 'score_mediane', 'vendor.1'], axis=1)
    df_activity = df.drop(
        ['id', 'emailmd5', 'emailsha256', 'customsubscriberid', 'ip', 'vendor', 'trackingcode', 'geocountry', 'geostate', 'geocity', 'geozipcode', 'subscriptiondate', 'birthdate', 'civility', 'country', 'density_per_km2',
         'score_individual_houses', 'em_creation', 'em_score', 'ip.1', 'num_msg', 'score_proprietaires', 'reoptindate', 'reoptinstatus', 'score_menages_imposes', 'score_pauvrete', 'score_mediane', 'source_url', 'timestamp', 'vendor.1',
         'zipcode'], axis=1)
    df_score = df.drop(
        ['id', 'emailmd5', 'emailsha256', 'customsubscriberid', 'ip', 'vendor', 'trackingcode', 'geocountry', 'geostate', 'geocity', 'geozipcode', 'lastactivity', 'lastemail', 'lastopenemail', 'lastclickemail', 'subscriptiondate',
         'birthdate', 'civility', 'country', 'em_creation', 'em_lastactivity', 'em_lastclick', 'em_lastmsg', 'em_lastopen', 'ip.1', 'num_msg', 'reoptindate', 'reoptinstatus', 'source_url', 'timestamp', 'vendor.1', 'zipcode'], axis=1)
    print(df_main_data)
    print(df_complements)
    print(df_activity)
    print(df_score)

    # engine = create_engine('postgresql+psycopg2://scott:tiger@localhost/mydatabase')
    connex = create_engine('postgresql+psycopg2://postgres:postgreskontiki@localhost/DW-Kontiki')
    df_main_data.to_sql(
        name='Main_DATAS',
        con=connex,
        index=False,
        if_exists='append'
    )


def count_elem_in_json():
    counter = 0
    print(type(glob.glob("..\\..\\Downloads\\DW\\DATAS EXPORT\\DATABASE\\Clubdesreducs\\edit file\\*.json")))
    for file_json in glob.glob("..\\..\\Downloads\\DW\\DATAS EXPORT\\DATABASE\\Clubdesreducs\\edit file\\*.json"):
        data_dict = json.loads(open(file_json, "r").read())
        for key, value in data_dict.items():
            if len(value) > 2:
                data_dict[key] = [elt for elt in value if str(elt) != 'nan']
        out_comprehension = [print(f"Find another at '{key}': {value}") for key, value in data_dict.items() if len(value) > 2]
    print(f"{len(out_comprehension)} founds")


def another_function():
    data_frame = pd.read_csv('../../Downloads/DW/DATAS EXPORT/DATABASE/LagendadesVP/file_CSV/LAGENDA_CARS_2020_01_09_11_45_17.csv', low_memory=False)
    print(data_frame.shape)


if __name__ == '__main__':
    # verification_of_repetition_data("../../Downloads/DW/DATAS EXPORT/DATABASE/Clubdesreducs/File_CSV", "../../Downloads/DW/DATAS EXPORT/DATABASE/Clubdesreducs/edit file/dict_json_sololearn.txt")
    # test_for_one()
    main_eleven_millions = read_eleven_million.DealElevenMillions()
    deal_obj = dealing_csv.DealCsv()
    # another_function()
    list_md5_cars_file_agenda_vp = main_eleven_millions.get_all_md5_infile_with_index(path='../../Downloads/DW/DATAS EXPORT/DATABASE/LagendadesVP/file_CSV/LAGENDA_CARS_2020_01_09_11_45_17.csv', index=0)
    main_eleven_millions.output_infile_write("..\..\Downloads\DW\DATAS EXPORT\DATABASE\LagendadesVP/file_CSV\edit file/test;txt", list_to_write_on_file=list_md5_cars_file_agenda_vp)