import requests


class APIStats:
    @staticmethod
    def prepare_export(user_api_key, base_id, segment_id, api_url='http://stats.kontikimedia.com/publicapi/prepeareExport'):
        data = {
            'userapikey': user_api_key,
            'baseid': base_id,
            'segmentid': segment_id
        }
        return requests.post(url=api_url, data=data).json()

    @staticmethod
    def check_export_request_status(user_api_key, request_id, api_url='http://stats.kontikimedia.com/publicapi/checkExportRequestStatus'):
        data = {
            'userapikey': user_api_key,
            'requestid': request_id
        }
        return requests.post(url=api_url, data=data).json()['url']


if __name__ == '__main__':
    print(APIStats.prepare_export('d47ee2abaeff90427b2898137c18cb50', 18, 2))