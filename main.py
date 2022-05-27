import argparse
import os
import shutil
from typing import List
import zipfile
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path

import psycopg2
import requests

HTTP_QUERY = "https://www.clinicaltrials.gov/api/query/full_studies?expr=&min_rnk=1&max_rnk=1&fmt=json"
EXTRACT_DIRECTORY = f"data/{datetime.today().strftime('%Y-%m-%d')}"


def download_file(url: str, abs_path_downloaded_file: str):
    """
    This method downloads the file.
    :param url: The url from which the file is to be downloaded
    :param abs_path_downloaded_file: The path where the file is to be saved
    :return: None
    """
    response = requests.get(url, stream=True)
    with open(abs_path_downloaded_file, "wb") as zip_file:
        for chunk in response.iter_content(chunk_size=1024):
            zip_file.write(chunk)


def unzip_file(file_path: str, extract_directory: str):
    """
    This method unzips the file.
    :param file_path: the path to the zip file
    :param extract_directory: the directory where the files are unzipped
    :return: None
    """
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        zip_ref.extractall(extract_directory)


def create_file(path: str):
    """
    This method creates the directory
    :param path: the path of the directory
    :return: None
    """
    try:
        os.makedirs(path)
    except FileExistsError:
        pass


def move_file(from_path: str, to_path: str):
    """
    This method moves the file from one directory to the other
    :param from_path: source file path
    :param to_path: destination file path
    :return: None
    """
    parent = str(Path(to_path).parent)
    if not os.path.exists(parent):
        create_file(parent)
    shutil.move(from_path, to_path)


class ETL(ABC):
    """
    The ETL abstract class
    """

    def __init__(self, conn, path: str, update_date: datetime):
        """

        :param conn: the database connection object
        :param path: the path of the file to extract
        :param update_date: the date which is received as response from DataVrs field
        """
        self.path = path
        self.connection = conn
        self.cursor = conn.cursor()
        self.counter = 0
        self.update_date = update_date
        self.file_list: List[str] = []
        self.done_file_path = os.path.join(str(Path(self.path).parent), "DONE", datetime.today().strftime('%Y-%m-%d'))

    @abstractmethod
    def extract(self, **kwargs) -> dict:
        pass

    @abstractmethod
    def transform(self):
        pass

    @abstractmethod
    def load(self, **kwargs):
        pass

    def update_database(self):
        """
        This method makes sure that the data from staging table
        is transformed into the clinic_trials table and then truncates
        the stg_table.
        :return: None
        """
        self.transform()
        self.cursor.execute("TRUNCATE TABLE STG_TRIALS")
        self.connection.commit()

    def post_update(self):
        """
        This method calls the update_database method, post
        which it moves the file from the extracted directory
        to the DONE folder. This is done so that, if the
        process fails in between, then only the remaining
        files from the extracted folder are processed.
        :return: None
        """
        self.update_database()
        for x in self.file_list:
            move_file(str(x), os.path.join(self.done_file_path, *str(x).split("\\")[-2:]))
        self.file_list[:] = []
        print(f"Inserted/Updated {self.counter} rows")

    def run(self):
        """
        This method links all the methods of the ETL class.
        i.e., the extract, load and transform
        Also, it commits data in the database after every
        1000 records.
        :return: None
        """
        for json_file_path in Path(self.path).rglob("*.json"):
            self.file_list.append(json_file_path)
            data, json_file_path = self.extract(json_file_path)
            self.load(**{'data': data, 'json_file_path': json_file_path,
                         'update_date': self.update_date.strftime("%Y-%m-%d")})

            self.counter += 1
            if self.counter % 1000 == 0:
                self.post_update()

        self.post_update()


class ClinicTrialETL(ETL):
    """
    This is the class that implements the ETL class.
    """

    def __init__(self, conxn, path: str, update_date: datetime):
        super().__init__(conxn, path, update_date)

    def extract(self, json_file_path: str):
        with open(json_file_path, 'r', encoding="UTF-8") as file:
            return file.read(), json_file_path

    def transform(self):
        """
        This method calls the sql query which merges data
        from stg table to the actual table
        :return: None
        """
        with open("MERGE.sql", "r") as sql_file:
            self.cursor.execute(sql_file.read())

    def load(self, data: str, json_file_path: str, update_date: str):
        """
        This method loads the json data into the stg table
        :param data: the json data
        :param json_file_path: the path of the file of the json data
        :param update_date: the date received as response from the API
        :return: None
        """
        data = data.replace("$", "")  # since the file could not contain $ for insertion into DB
        self.cursor.execute(f"""INSERT INTO STG_TRIALS VALUES ($${data}$$, '{json_file_path}','{update_date}')""")


if __name__ == '__main__':
    """
    This method takes optional argument
    --dataset_dir, which is the parent 
    directory from where the json is to
    be read. If this argument is not 
    provided, then the process go on to
    check if there is any new data available
    and downloads it in the data folder, 
    unzips it and starts the ETL process. 
    """

    conn = psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="Australia@89")

    parser = argparse.ArgumentParser(description='ClinicTrials ETL')
    parser.add_argument("--dataset_dir", required=False)
    args = parser.parse_args()

    res_date = datetime.strptime(requests.get(HTTP_QUERY).json().get("FullStudiesResponse").get("DataVrs"),
                                 "%Y:%m:%d %H:%M:%S.%f")

    if args.dataset_dir:
        try:
            ct = ClinicTrialETL(conn, args.dataset_dir, res_date)
            create_file(ct.done_file_path)
            ct.run()
        finally:
            conn.close()
    else:
        database_date = datetime.strptime(
            conn.cursor().execute("select coalesce(max(update_date), '1900-01-01') from clinic_trials")[0], "%Y-%m-%d")
        if res_date > database_date:
            download_file(url="https://clinicaltrials.gov/api/gui/ref/download_all", abs_path_downloaded_file="data")
            create_file(EXTRACT_DIRECTORY)
            create_file(os.path.join(EXTRACT_DIRECTORY, "DONE"))
            unzip_file(file_path="data/AllAPIJSON.zip", extract_directory=EXTRACT_DIRECTORY)
            try:
                ct = ClinicTrialETL(conn, EXTRACT_DIRECTORY, res_date)
                create_file(ct.done_file_path)
                ct.run()
            finally:
                conn.close()
