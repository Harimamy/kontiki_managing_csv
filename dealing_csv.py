from app_admin_dwh.manage_csv import connect_pg
from concurrent.futures import ThreadPoolExecutor
import time
import threading


class DealCsv(object):
    def get_column_csv(self):
        pass

    def get_to_pg(self, query):
        connexion, cursor = connect_pg.postgres_connect()
        cursor.execute(query)
        rows = cursor.fetchall()
        connexion.close()
        return rows

    def insert_to_pg(self, query):
        try:
            connexion, cursor = connect_pg.postgres_connect()
            cursor.execute(query)
            connexion.commit()
        except IOError:
            connexion.rollback()
            print("IOError exception")
        except Exception:
            raise ValueError("ValueError catch by Harry")
        finally:
            connexion.close()
            cursor.close()

    # unused method create_table in datawarehouse
    # def create_table_dataware(self):
    #     connexion, cursor = connect_pg.postgres_connect()
    #     with open("../../Downloads/DW/Clubdesreducs_CARS_2019_10_25(1).csv", "r", encoding="utf-8-sig") as content_file:
    #         data = content_file.readlines()
    #         truth_data = data[0].strip().split(",")

    def threading_insert(self, path, a, b):
        with open(path, "r", encoding="utf-8-sig") as file:
            count = 0
            for line in file:
                if a <= count < b:
                    try:
                        list_data = line.strip().split(",")
                        correct_name_geo_city = ""
                        for letter in list_data[9]:
                            if letter == "'":
                                correct_name_geo_city += "''"
                            else:
                                correct_name_geo_city += letter
                        list_data[9] = correct_name_geo_city
                        query = Deal_obj.deal_cleaning(list_data)
                        Deal_obj.insert_to_pg(query)
                        print("successfull insertion at ligne ", count)
                    except Exception:
                        raise ValueError("Value error raising by Harry on line ", count)
                count += 1

    def reading_csv(self, path):
        with open(path, "r", encoding="latin1") as file:
            data = file.readlines()[1:]
            file.close()
        return data

    @staticmethod
    def deal_cleaning(truth_data):
        print(truth_data)
        index = 0
        for val_column in truth_data:
            if val_column == '':
                # print("null value find at", index)
                truth_data[index] = 'NULL'
            index += 1

        character_strong, list_index_int = "", [0, 25, 27]
        for val in truth_data:
            if truth_data.index(val) in list_index_int:
                character_strong += "{}, "
            else:
                if val == 'NULL':
                    character_strong += "{}, "
                else:
                    character_strong += "\'{}\', "

        request_insert = "INSERT INTO \"DWH\".\"DATAS_clubdesreducs_test\" " \
                         "(id, emailmd5, emailsha256, customsubscriberid, ip, vendor, trackingcode, geocountry, " \
                         "geostate, geocity, geozipcode, lastactivity,lastemail, lastopenemail, lastclickemail," \
                         "subscriptiondate, birthdate, carrefour, civility, country, em_creation, em_lastactivity," \
                         "em_lastclick, em_lastmsg, em_lastopen, em_score, ip_other, num_msg, orangepack, " \
                         "prefonjul18, reoptindate, reoptinstatus, source_url, \"timestamp\", vendor_other, zipcode)" \
                         " VALUES({})".format(character_strong[:-2].format(*truth_data))
        return request_insert


if __name__ == '__main__':
    start_time = time.time()
    Deal_obj = DealCsv()
    # executor = ThreadPoolExecutor(max_workers=300)
    data = Deal_obj.reading_csv("../../Downloads/DW/DATAS EXPORT/CSV/Clubdesreducs_CARS_2019_10_25.csv")
    print("number of line ", len(data))
    n = 40
    number_thread = int(len(data) / n)
    list_ = [val for val in range(0, len(data), number_thread)]
    list_[-1] = list_[-1] + int(len(data) % n)

    thread = []
    i = 0
    j = 1
    for a, b in enumerate(list_):
        while i <= a:
            if j > a:
                break
            else:
                process = threading.Thread(target=Deal_obj.threading_insert, args=["../../Downloads/DW/DATAS EXPORT/CSV/Clubdesreducs_CARS_2019_10_25.csv", list_[i], list_[j]])
                process.start()
                thread.append(process)
                i += 1
                j += 1
    for t in thread:
        t.join()

    print("number thread", number_thread, "list -->", list_)
    # problème first insert at ligne index 37445, 51353 data[51353:]

    # counter = 0
    # for mail_md5 in data:
    #     counter += 1
    #     try:
    #         list_data = mail_md5.strip().split(",")
    #         correct_name_geo_city = ""
    #         for letter in list_data[9]:
    #             if letter == "'":
    #                 correct_name_geo_city += "''"
    #             else:
    #                 correct_name_geo_city += letter
    #         list_data[9] = correct_name_geo_city
    #         query = Deal_obj.deal_cleaning(list_data)
    #         print(query)
    #         Deal_obj.insert_to_pg(query)
    #         print("successful insertion at ligne ", counter)
    #         # if counter == 10:
    #         #     break
    #     except Exception:
    #         # pass
    #         raise ValueError("Value error raising by Harry on line ", counter)
    print("execution time : %s secondes ---" % (time.time() - start_time))