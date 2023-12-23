# %%
import numpy as np
import pandas as pd
from itertools import combinations
import matplotlib.pyplot as plt
import networkx as nx
from sklearn.preprocessing import MinMaxScaler
from sklearn.feature_extraction.text import CountVectorizer, TfidfVectorizer
from sklearn.decomposition import LatentDirichletAllocation

# %%
class DataExploration():
    
    def __init__(self, data_path = "../data/processed.parquet"):
        self.data_path = data_path

    def load_data(self):
        self.data = pd.read_parquet(self.data_path)
        self.data = self.data.loc[(self.data.n_chars>200) & (self.data.language=="en")\
            & (self.data.language_score>=0.99),:]
        return self
    
    def construct_ngram_stats(self, column_name="reconstructed_text", range=(1,1)):
        cv = CountVectorizer(ngram_range=range, max_features=10000)
        ng_counts = cv.fit_transform(self.data[column_name])
        ng_counts = pd.DataFrame(ng_counts.todense(),
            columns=cv.get_feature_names_out())
        ng_counts = ng_counts.sum().reset_index()
        ng_counts.columns = ["ngram", "frequency"]
        ng_counts["n"] = ng_counts.ngram.apply(lambda x: len(x.split(" ")))
        self.ngram_stats = ng_counts.loc[:,["n","ngram", "frequency"]]
        return self
    
    def plot_ngram_stats(self, top_n=15):
        c, r = 2, int(np.ceil(self.ngram_stats.n.max()/2))
        f, axs = plt.subplots(r,c,figsize=(7.5*c, 5.75*r))
        for ax, n in zip(axs.flatten(), self.ngram_stats.n.unique()):
            self.ngram_stats[self.ngram_stats.n==n]\
                .sort_values("frequency").tail(top_n).\
                    plot(y="frequency",x="ngram", kind="barh", ax=ax, legend="false",
                        logx=True);
            ax.set_title(str(n)+"-gram");
            ax.set_ylabel("");
            ax.set_xlabel("frequency");
            ax.get_legend().remove();
        f.tight_layout();
        f.suptitle("");
        return ax
    
    def _plot_cat_column(self, column_name, top_n=25, title=None, ax=None):
        if ax is None:
            ax = plt.gca()
        if title is None:
            title = column_name.lower()
        self.data[column_name].value_counts()\
            .sort_values().tail(top_n).plot(kind="barh", ax=ax)
        ax.set_xlabel("frequency")
        ax.set_ylabel(title)
        return ax
    
    def _plot_num_column(self, column_name, title=None, ax=None):
        if ax is None:
            ax = plt.gca()
        if title is None:
            title = column_name.lower()            
        self.data[column_name].plot(kind="hist", rot=90, ax=ax)
        ax.set_ylabel("frequency")
        ax.set_xlabel(title)
        return ax
    
    def plot_covariates(self, top_n=20, figsize=(20,7)):
        f, axs = plt.subplots(1,3,figsize=figsize)
        self._plot_cat_column("country", top_n=top_n, ax=axs[0])
        self._plot_cat_column("sector", top_n=top_n, ax=axs[1])
        self._plot_cat_column("type", top_n=top_n, ax=axs[2])
        f.tight_layout()
        return axs
    
    def plot_text_characteristics(self, figsize=(15,5)):
        f, axs = plt.subplots(1,3,figsize=figsize)
        key_dict = {"n_chars": "number of characters", "n_words": "number of words",
                    "n_sentences": "number of sentences"}
        for c, ax in zip(key_dict.keys(), axs.flatten()):
            self._plot_num_column(c, key_dict[c], ax)
        f.tight_layout()
        return axs
    
    def construct_cooc_stats(self, column_name="reconstructed_text"):
        coovec = TfidfVectorizer(ngram_range=(1,1), max_features=10000)
        coo_w = coovec.fit_transform(self.data[column_name])
        tokens = coovec.get_feature_names_out()
        coo_w = coo_w.T.dot(coo_w)
        coo_w = np.triu(coo_w.todense(), k=1)
        edges = list(combinations(range(coo_w.shape[0]),2))
        ind0, ind1 = [e[0] for e in edges], [e[1] for e in edges]
        freq = coo_w[ind0, ind1]
        self.cooc_stats  = pd.DataFrame(columns=["from", "to", "weight"])
        self.cooc_stats ["from"], self.cooc_stats ["to"] = [tokens[i]for i in ind0], [tokens[i]for i in ind1]
        self.cooc_stats ["weight"] = freq
        self.cooc_stats ["weight"] = MinMaxScaler()\
            .fit_transform(self.cooc_stats [["weight"]])
        return self
    
    def plot_cooc_stats(self, figsize=(20,20)):
        net = nx.convert_matrix.from_pandas_edgelist(
            self.cooc_stats.sort_values("weight").tail(1000),
                source="from", target="to", edge_attr="weight")
        f, ax = plt.subplots(1,1, figsize=figsize)
        pos = nx.kamada_kawai_layout(net)
        nx.draw_networkx_labels(net, pos, font_size=10,
            font_family="sans-serif", alpha=1, ax=ax);
        nx.draw_networkx_edges(net, width=[net[u][v]["weight"]*10 for u,v in net.edges()],
            pos=pos, alpha=.05)
        ax.set(frame_on=False)
        return ax
    
    def _construct_tf(self, column_name="reconstructed_text"):
        tfv = CountVectorizer(max_features=10000)
        self.tf_data =  tfv.fit_transform(self.data[column_name])
        self.tf_model = tfv
        return self
    
    def _construct_lda(self, n_topics=15):
        lda = LatentDirichletAllocation(n_components=n_topics, max_iter=10,
        learning_method="online", learning_offset=50., random_state=0)
        lda.fit(self.tf_data)
        self.lda_model = lda
        return self
    
    def construct_lda(self, column_name="reconstructed_text", n_topics=15):
        self._construct_tf(column_name)
        self._construct_lda(n_topics)
        return self
    
    # NOTE: generalize the plotting function
    def plot_lda_top_words(self, top_n=15, figsize=(20,20)):
        f, axs = plt.subplots(3, 5, figsize=figsize)
        axs = axs.flatten()
        for topic_idx, topic in enumerate(self.lda_model.components_):
            top_features_ind = topic.argsort()[:-top_n - 1:-1]
            top_features = [self.tf_model.get_feature_names_out()\
                [i] for i in top_features_ind]
            weights = topic[top_features_ind]
            ax = axs[topic_idx]
            ax.barh(top_features, weights, height=0.7)
            ax.set_title(f"Topic {topic_idx +1}",
                fontdict={"fontsize": 12})
            ax.invert_yaxis();
            ax.tick_params(axis="both", which="major", labelsize=10)
            for i in "top right left".split():
                ax.spines[i].set_visible(False)
            f.suptitle("", fontsize=14)
        plt.subplots_adjust(top=0.90, bottom=0.05, wspace=0.90, hspace=0.3)
        return axs    