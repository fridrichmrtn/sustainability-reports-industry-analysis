### prepare
import pandas as pd
import re
import os
import requests
import magic
from joblib import Parallel, delayed
import pickle

class DataIngestion():
    
    def __init__(self,
                docs_file = "../../data/unglobalcompact.csv",
                raw_folder = "../../data/raw_files/",
                data_folder = "../../data/") -> None:
        self.docs_file = docs_file
        self.raw_folder = raw_folder
        self.data_folder = data_folder
        pass

    def _sanitize_name(self, name):
        name = re.sub(r"[^a-zA-Z ]","", name).lower()
        return re.sub(r"[ ]+", "_", name)
    
    def _get_file_atts(self, path):
        dir = os.path.dirname(path)
        base_name = os.path.basename(path)
        name, extension = os.path.splitext(base_name)
        return {"dir" : dir,
                "name" : name,
                "ext" : extension.split("?")[0]}
    
    def _get_destination(self, row, dir=None):
        atts_dict = self._get_file_atts(row["communication_on_progress_file"])
        index = str(row.name)
        company =self._sanitize_name(row["name"])
        ext = atts_dict["ext"]
        if dir is None:
            dir = ""
        return dir + "_".join([index, company]) + ext
    
    def load_docs(self):
        self.docs_data = pd.read_csv(self.docs_file)   
        new_columns = {c: self._sanitize_name(c) for c in self.docs_data.columns}
        self.docs_data = self.docs_data.rename(columns=new_columns)
        url_rows = self.docs_data.communication_on_progress_file.notnull()
        self.docs_data = self.docs_data.loc[url_rows,
                ["name","type", "country", "sector", "communication_on_progress_file"]]
        self.docs_data["file_destination"] = self.docs_data\
            .apply(self._get_destination, dir=self.raw_folder, axis=1)
        return self
        
    def _download_file(self, url, file_path):
        if os.path.exists(file_path):
            print(f"File on '{file_path}' already exists. Skipping download.")
            return None
        try:
            response = requests.get(url, stream=True)
            response.raise_for_status()
            with open(file_path, 'wb') as file:
                for chunk in response.iter_content(chunk_size=8192):
                    file.write(chunk)
            print(f"File on '{file_path}' downloaded successfully.")
            return file_path
        except requests.RequestException as e:
            print(f"Error downloading '{file_path}': {e}.")
            return None

    def download_reports(self):
        url_list = self.docs_data["communication_on_progress_file"].values
        path_list = self.docs_data["file_destination"].values
        Parallel(n_jobs=5)(delayed(self._download_file)\
            (url, path) for url, path in zip(url_list, path_list))
        return self
    
    def _check_path(self,path):
        if os.path.exists(path):
            return path
        else:
            return None

    def _get_conversion_path(self,file_destination, dir_name=None):
        if dir_name==None:
            dir_name = os.path.dirname(file_destination)
        base_name = os.path.basename(file_destination)
        name, extension = os.path.splitext(base_name)
        return os.path.join(*[dir_name, name+".pdf"])

    def _get_metadata(self):
        self.docs_data["file_type"] = self.docs_data\
            .apply(lambda x:magic.from_file(x["file_destination"],mime=True), axis=1)
        self.docs_data["file_size"] = self.docs_data\
            .apply(lambda x:os.path.getsize(x["file_destination"])/10**6, axis=1)
        self.docs_data["converted_file_destination"] = None
        pdf_rows = self.docs_data.file_type=="application/pdf"
        self.docs_data.loc[pdf_rows, "converted_file_destination"] = self.docs_data.loc[pdf_rows, "file_destination"]
        conv_rows_ind = self.docs_data.loc[
            (self.docs_data.converted_file_destination.isnull())
            & (~self.docs_data.file_type.isin(["inode/x-empty", "application/octet-stream"]))
            & (self.docs_data.file_size<90),].index
        self.docs_data["conversion"] = False
        self.docs_data.loc[conv_rows_ind, "conversion"] = True
        return self

    def _convert_row(self, ind):
        file_destination = self.docs_data.loc[ind, "file_destination"]
        converted_file_destination = self._get_conversion_path(file_destination)
        converted_file_dir = os.path.dirname(converted_file_destination)
        # NOTE: make sure this works
        if os.path.exists(converted_file_destination):
            print(f"File on '{converted_file_destination}' already exists. Skipping conversion.")
            self.docs_data.loc[ind, "converted_file_destination"] = converted_file_destination
            return self
        try:
            cmd = f"libreoffice --headless --convert-to pdf {file_destination} --outdir {converted_file_dir}"
            status = " ".join(os.popen(cmd).readlines())
            if "error" in status.lower():
                raise Exception(status)
            self.docs_data.loc[ind, "converted_file_destination"] = self._check_path(converted_file_destination)
            print(f"File on '{converted_file_destination}' converted successfully.")
        except Exception as e:
            print(f"Error converting '{file_destination}': {e}.")
        return self

    def convert_reports(self):
        self = self._get_metadata()
        for ind in self.docs_data.loc[self.docs_data.conversion,].index:
            self = self._convert_row(ind)
        return self

    def _get_txt_path(self, file_path,
            dir_name="../../data/txt_files/"):
        base_name = os.path.basename(file_path)
        name, extension = os.path.splitext(base_name)
        return os.path.join(*[dir_name, name+".txt"])
    
    def _read_row(self, ind, overwrite=False):
        row = self.docs_data.loc[ind,]
        row["txt_file_destination"] = None
        pdf_path = row["converted_file_destination"]
        txt_path = self._get_txt_path(pdf_path)
        if os.path.exists(txt_path) and not overwrite:
            print(f"File on '{txt_path}' already exists. Skipping reading.")
            row["txt_file_destination"] = txt_path
            return row
        else:
            print(f"Reading '{row['converted_file_destination']}'.")
            try:
                #txt = self._pdf2txt(pdf_path)
                #self._txt2file(txt, txt_path)
                cmd = f"gs -sDEVICE=ocr -r200 -dQUIET -dBATCH -dNOPAUSE -sOutputFile={txt_path} {pdf_path} > conversion.log 2>&1"
                status = os.system(cmd)
                row["txt_file_destination"] = self._check_path(txt_path)
            except Exception as e:
                print(f"Error reading '{pdf_path}': {e}.")
        return row

    def read_reports(self, overwrite=False):
        rows_ind = self.docs_data.index[self.docs_data.converted_file_destination.notnull()]
        rows_ls = Parallel(n_jobs=7)(delayed(self._read_row)\
            (ind, overwrite) for ind in rows_ind)
        self.docs_data = self.docs_data.merge(
            pd.DataFrame(rows_ls)[["txt_file_destination"]],
            how="left", left_index=True, right_index=True)
        return self

    def save_data(self, file_path=None):
        if file_path is None:
            file_path = self.data_path+"ingested.pkl"
        with open(file_path, "wb") as file:
            pickle.dump(self.docs_data, file)
        return self        

# %%
# Ingestion = DataIngestion().load_docs().download_reports()\
#     .convert_reports().read_reports().save_data()