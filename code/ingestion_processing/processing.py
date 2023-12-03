# %%
# %%
### prepare
import os
import pandas as pd
import re
from joblib import Parallel, delayed
import pycld2 as cld2
import spacy
import gc

class DataProcessing():
    
    def __init__(self,
                data_file = "../../data/ingested.parquet",
                data_folder = "../../data/") -> None:
        self.data_file = data_file
        self.data_folder = data_folder
        pass

    def load_data(self):
        self.data = pd.read_parquet(self.data_file)
        #self.data = self.data.iloc[:100,:]
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
        row = self.data.loc[ind].copy()
        #row["raw_text"] = self._load_text(row["txt_file_destination"])
        row["text"] = self._preprocess_text(
            self._load_text(row["txt_file_destination"]))
        return row
        
    def preprocess_reports(self, n_jobs = 8):
        self.data = self.data.loc[(self.data.txt_file_destination.notnull()),]
        rows_ls = Parallel(n_jobs = n_jobs)(delayed(self._preprocess_row)\
            (ind) for ind in self.data.index)
        self.data = pd.DataFrame(rows_ls)
        return self
    
    def _check_path(self,path):
        if os.path.exists(path):
            return path
        else:
            return None    

    def _get_parquet_files(self, dir_name):
        for file in os.listdir(dir_name):
            yield pd.read_parquet(os.path.join(dir_name, file))        

    def _get_batches(self, data, batch_size = 100):
        for i in range(0, len(data), batch_size):
            #print("Processing batch: ", i)
            yield data.iloc[i:i+batch_size,:].copy()

    def _deconstruct_upos_batch(self, data, col="text", n_jobs=1):
        nlp = spacy.load("en_core_web_lg", disable=["ner", "parser"])
        nlp.max_length = 20000000
        docs = nlp.pipe(data.loc[:,col].values, n_process=n_jobs, batch_size=3)
        parsed_ls = [(data.index[i], t.text, t.lemma_, t.pos_, t.tag_, t.dep_,
                t.shape_, t.is_alpha, t.is_stop) for i, parsed in enumerate(docs)
                    for t in parsed]
        return pd.DataFrame(parsed_ls,
            columns=["doc_id","text", "lemma", "pos", "tag",
                "dep", "shape", "is_alpha", "is_stopword"])

    def _deconstruct_save_upos_batch(self, data, dir_name, col="text", n_jobs=1):
        batches = self._get_batches(data)
        for i, v in enumerate(batches):
            upos = self._deconstruct_upos_batch(v, col, n_jobs)
            upos.to_parquet(dir_name + str(i) + ".parquet")
            del upos;gc.collect()
        return self
    
    def _filter_upos(self, upos):
        # univariate filter
        upos = upos.loc[upos.pos.isin(["NOUN", "ADJ", "VERB"]),:] 
        upos = upos.loc[~upos.is_stopword,:]
        upos = upos.loc[(upos.lemma.str.len()>2) & (upos.lemma.str.len()<19),:]
        # multivariate filter
        lemma_stats = upos.groupby("lemma", as_index=False).agg({"doc_id":["count", "nunique"]})
        pf = (lemma_stats[("doc_id","count")]>500)&(lemma_stats[("doc_id","nunique")]>250) #500,250
        stopword_set = set([])
        lemma_set = set(lemma_stats.loc[pf,"lemma"].values).difference(stopword_set)
        return upos.loc[upos.lemma.isin(lemma_set),:]

    def _reconstruct_upos(self, upos, col="reconstructed_text"):
        # reconstruct text
        reconstructed = pd.DataFrame(upos.groupby("doc_id")\
            .apply(lambda x:" ".join(x["lemma"])), columns=[col])
        # clean up
        reconstructed[col] = reconstructed[col].apply(\
            lambda x: re.sub(r'\b(\w+\s*)\1{1,}', '\\1', x))  
        return reconstructed

    def construct_upos(self, n_jobs=8, dir_name="../../data/upos_files/", col="text"):
        self.data = self.data.loc[(self.data.loc[:,col].notnull())\
            & (~self.data.loc[:,col].isin([""])),]
        # deconstruct in parallel and save
        self._deconstruct_save_upos_batch(self.data, dir_name, col, n_jobs)
        # load back
        upos = pd.concat(self._get_parquet_files(dir_name)) 
        # filter
        upos = self._filter_upos(upos)
        # reconstruct text and merge back
        self.data = self.data.merge(self._reconstruct_upos(upos),
            how="inner", left_index=True, right_index=True)   
        return self
    
    def _metadata_row(self, ind, col = "reconstructed_text"):
        row = self.data.loc[ind].copy()
        row["n_chars"] = len(row[col])
        row["n_words"] = len(re.split("\w+",row[col]))
        row["n_sentences"] = len(re.split(r"[.?!]", row[col]))

        lang_estimation = cld2.detect(row[col], returnVectors=True)[2]
        row["language"] = lang_estimation[0][1]
        row["language_score"] = lang_estimation[0][2]/100.0
        return row    
    
    def get_metadata(self, col="reconstructed_text", n_jobs=4):
        self.data = self.data.loc[(self.data.loc[:,col].notnull())\
            & (~self.data.loc[:,col].isin([""])),]
        rows_ls = Parallel(n_jobs = n_jobs)(delayed(self._metadata_row)\
            (ind, col) for ind in self.data.index)
        self.data = pd.DataFrame(rows_ls)
        return self
    
    def save_data(self, file_path = None):
        if file_path is None:
            file_path = self.data_folder+"processed.parquet"
        self.data.to_parquet(file_path)
        return self    

# %%
Processing = DataProcessing().load_data().preprocess_reports()\
    .construct_upos().get_metadata().save_data()
