import logging
from googleapiclient.discovery import build
from oauth2client.service_account import ServiceAccountCredentials
from itertools import zip_longest


class GSheet:
    def __init__(self, key_file_location):
        self.scopes = ["https://www.googleapis.com/auth/spreadsheets.readonly"]
        credentials = ServiceAccountCredentials.from_json_keyfile_name(
            key_file_location, scopes=self.scopes
        )
        self.service = build("sheets", "v4", credentials=credentials)

    def read(self, sheet_config):
        logging.info(f"config: {sheet_config}")
        sheet_id = sheet_config["id"]
        range = sheet_config["range"]
        has_header = sheet_config.get("has_header", True)

        sheet = self.service.spreadsheets()
        result = sheet.values().get(spreadsheetId=sheet_id, range=range).execute()
        values = result.get("values", [])

        if not values:
            logging.warning("No data found.")
        if has_header and len(values) > 0:
            headers = values.pop(0)
            header_len = len(headers)
            values = [
                dict(list(zip_longest(headers, row))[:header_len]) for row in values
            ]
        return values
