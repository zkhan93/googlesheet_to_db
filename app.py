import sys
import os
import json
import logging
from source.gsheet import GSheet
from target.database import Database


logging.basicConfig(
    format="%(asctime)s:%(levelname)s:%(filename)s:%(lineno)d: %(message)s",
    level=logging.INFO,
    stream=sys.stdout,
)


def read_service_creds(key_file_location):
    if not os.path.exists(key_file_location):
        data = os.getenv("CREDENTIALS_SERVICE_ACCOUNT")
        with open(key_file_location, "w") as f:
            f.write(data)


def get_configurations():
    with open("config.json", "r") as config:
        return json.loads(config.read())


def run():
    configs = get_configurations()
    logging.info(f"{len(configs)} configs found")

    key_file_location = "service.credentials.json"
    read_service_creds(key_file_location)

    database_url = os.environ["DATABASE_URL"]
    logging.info(database_url)

    service = GSheet(key_file_location)
    db = Database(database_url)
    result = {}
    for config in configs:
        name = config["name"]
        try:
            read_config = config["sheet"]
            write_config = config["database"]
        except KeyError:
            message = f"Invalid config {config}"
            logging.exception(message)
            result[name] = message
        try:
            data = service.read(read_config)
            logging.info(f"read success {len(data)} rows read")
        except Exception:
            message = f"Reading sheet failed: {read_config}"
            logging.exception(message)
            result[name] = message
        else:
            try:
                db.write(data, write_config)
                logging.info("write success")
            except Exception:
                message = f"failed writing to database {write_config}"
                logging.exception(message)
                result[name] = message
            else:
                result[name] = "Loaded successfully"


if __name__ == "__main__":
    run()
