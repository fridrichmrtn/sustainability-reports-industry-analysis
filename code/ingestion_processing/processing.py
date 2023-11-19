# %%
### prepare
import pandas as pd
import re
from joblib import Parallel, delayed
import pycld2 as cld2

class DataProcessing():
    
    def __init__(self,
                docs_file = "../../data/ingested.parquet",
                data_folder = "../../data/") -> None:
        self.docs_file = docs_file
        self.data_folder = data_folder
        pass

    def load_data(self):
        self.docs = pd.read_parquet(self.docs_file)
        return self
    
    def _load_text(self, file_path):
        if file_path is None:
            return None
        with open(file_path, "r") as f:
            text = f.read()
        return text

    def _preprocess_text(self, text):
        text = text.lower()
        text = re.sub(r"<.*?>|</.*?>","", text)
        text = re.sub(r"(s?)(f|ht)tp(s?)://\S+\b","", text)
        text = re.sub(r"^[a-z0-9]+[\._]?[a-z0-9]+[@]\w+[.]\w{2,3}$","", text) #email
        text = re.sub(r"\\-","", text)
        text = re.sub("[^a-z '.,?!:]"," ", text)
        text = re.sub(r"\b(\w+\s*)\1{1,}", " ", text) #dupli "\\1"
        return re.sub(r" +"," ", text)
    
    def _preprocess_row(self, ind):
        # preprocess
        row = self.docs.loc[ind].copy()
        row["raw_text"] = self._load_text(row["txt_file_destination"])
        row["text"] = self._preprocess_text(row["raw_text"])
        return row
    
    def _metadata_row(self, ind):
        row = self.docs.loc[ind].copy()
        row["n_chars"] = len(row["text"])
        row["n_words"] = len(re.split("\w+",row["text"]))
        row["n_sentences"] = len(re.split(r"[.?!]", row["text"]))

        lang_estimation = cld2.detect(row["text"], returnVectors=True)[2]
        row["language"] = lang_estimation[0][1]
        row["language_score"] = lang_estimation[0][2]/100.0
        return row    
        
    def preprocess_reports(self, n_jobs = 8):
        self.docs = self.docs.loc[(self.docs.txt_file_destination.notnull()),]
        rows_ls = Parallel(n_jobs = n_jobs)(delayed(self._preprocess_row)\
            (ind) for ind in self.docs.index)
        self.docs = pd.DataFrame(rows_ls)
        return self
    
    def get_metadata(self, n_jobs = 1):
        self.docs = self.docs.loc[(self.docs.text.notnull()) & (~self.docs.text.isin([""])),]
        rows_ls = Parallel(n_jobs = n_jobs)(delayed(self._metadata_row)\
            (ind) for ind in self.docs.index)
        self.docs = pd.DataFrame(rows_ls)
        return self
    
    def save_data(self, file_path = None):
        if file_path is None:
            file_path = self.data_folder+"processed.parquet"
        self.docs.to_parquet(file_path)
        return self    

# %%
# Processing = DataProcessing().load_data().preprocess_reports()\
#     .get_metadata().save_data()


