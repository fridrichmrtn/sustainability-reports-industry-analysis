library(R6)
library(arrow)
library(dplyr)
library(stm)
library(parallel)
library(stringi)
library(data.table)

stm_exploration <- R6Class("stm_exploration_class",
                           
  public = list(
    input_data_path = NULL,
    sweep_data_dir = NULL,
    sweep_range = NULL,
    export_data_dir = NULL,
    seed = NULL,
    data = NULL,
    preprocessed_data = NULL,
    sweep_data = NULL,
    
    initialize = function(input_data_path, sweep_data_dir, sweep_range, export_data_dir, seed=42) {
      self$input_data_path = input_data_path
      self$sweep_data_dir = sweep_data_dir
      self$sweep_range = sweep_range
      self$export_data_dir = export_data_dir
      self$seed = seed
    },    
    
    load_data = function(data_path=self$input_data_path) {
      self$data = arrow::read_parquet(data_path)
    },
    
    filter_data = function() {
      self$data = self$data %>%
        rename(index = `__index_level_0__`) %>%
        select(index, name, type, country, sector, reconstructed_text) %>%
        filter(sector != "Not Applicable")
    },
    
    preprocess_data = function() {
      processed = stm::textProcessor(
        documents = self$data$reconstructed_text,
        metadata = self$data,
        lowercase = FALSE,
        removepunctuation = FALSE,
        removenumbers = FALSE,
        removestopwords = FALSE,
        stem = FALSE)
      
      self$preprocessed_data = stm::prepDocuments(
        processed$documents,
        processed$vocab,
        processed$meta)
    },

    fit_model = function(K, seed, formula=NULL, em_its=750) {
      return(
        stm::stm(documents=self$preprocessed_data$documents,
            vocab=self$preprocessed_data$vocab,
            data=self$preprocessed_data$meta,
            prevalence=formula,
            verbose=F,
            K=K, seed=seed,
            max.em.its=em_its,
            init.type="Spectral"))
    },
    
    eval_model = function(model, M=50) {
      require(magrittr)
      sc = stm::semanticCoherence(
        model, self$preprocessed_data$documents, M=M) %>% mean()
      frex = stm::exclusivity(model, M=M) %>% mean()
      em = model$convergence$bound[model$convergence$its]/model$settings$dim$V
      return(list(sc=sc, frex=frex, ex=em))
    },
    
    parameter_sweep = function(topic_range=self$sweep_range) {
      
      save_model = function(model, K, dump_dir) {
        dir.create(dump_dir, recursive=TRUE, showWarnings=FALSE)
        file_name = paste0(stringi::stri_pad_left(K, width=3, pad="0"), "_topics_model.rds")
        file_path = file.path(dump_dir, file_name)
        saveRDS(model, file_path)
        return(file_path)
      }
      
      wrap_fit_eval = function(K, dump_dir){
        print(paste0("Fitting and evaluating model with ", K, " topics..."))
        st =  Sys.time()
        model = self$fit_model(K, self$seed)
        file_path = save_model(model, K, dump_dir)
        eval = self$eval_model(model)
        rt = Sys.time()-st
        eval$K = K
        eval$time = rt
        eval$file_path = file_path
        print(paste0("Finished fitting and evaluating model with ", K, " topics in ", format(rt, digits=3), "."))
        return(eval)
      }
      
      format_sweep_data = function(sweep_data) {
        colnames(sweep_data) = NULL
        sweep_data = as.data.frame(t(sweep_data))
        sweep_data$K = as.numeric(sweep_data$K)
        sweep_data$sc = as.numeric(sweep_data$sc)
        sweep_data$frex = as.numeric(sweep_data$frex)
        sweep_data$time = as.numeric(sweep_data$time)
        return(sweep_data)
      }
      
      print("Starting hyperparam sweep for number of topics...")
      st =  Sys.time()
      cl = makeCluster(4)
      sweep_data = parSapply(cl, topic_range, FUN=wrap_fit_eval, dump_dir=self$sweep_data_dir)
      stopCluster(cl)
      print(paste0("The procedure finished in ", format(Sys.time()-st, digits=2) ,"."))
      
      self$sweep_data = format_sweep_data(sweep_data)
      self$sweep_range = topic_range
    },
    
    plot_sweep = function() {
      
      scale_linear = function(x){
        x_range = range(x)
        scale_linear = (x-x_range[1])/(x_range[2]-x_range[1])
        return(scale_linear)}
  
      sweep_data = test$sweep_data
      sweep_data[,c("semcoh_scaled", "frex_scaled")] = apply(
          sweep_data[,c("sc","frex")], 2, scale_linear)
      
      sweep_data$dist = ((1-sweep_data$frex_scaled)^2+(1-sweep_data$semcoh_scaled)^2)^(1/2)
      max_dist = arrange(sweep_data, dist) %>% select(dist) %>% slice(1) %>% unlist()
      
      plot(x=sweep_data$semcoh_scaled, y=sweep_data$frex_scaled, type='n',
           main="",xlab='semantic coherence', ylab='frequency and exclusivity',
           cex.main=0.8, cex.axis=0.8, cex.lab=0.8)
      
      for(r in 1:nrow(sweep_data)){
        text(x=sweep_data$semcoh_scaled[r], y=sweep_data$frex_scaled[r], label=sweep_data$K[r],
             cex=0.75, col = ifelse(sweep_data$dist[r]<=max_dist,"red","black"))}      
    },
    
    load_model = function(model_path) {
      return(readRDS(model_path))
    },
    
    get_model_info = function(model) {
      model_info = sprintf(
        "We fit a topic model with %i topics, %i documents and a %i word dictionary.\nIn addition, the model's semantic coherence is %f and its exclusivity is %f. \n", 
        model$settings$dim$K, model$settings$dim$N, model$settings$dim$V,
        mean(stm::semanticCoherence(model, self$preprocessed_data$documents, M=50)), mean(stm::exclusivity(model, M=50)))
      return(model_info)
    },
    
    plot_topic_prevalence = function(model) {
      topic_prevalence = data.frame(topic=paste0("Topic ", 1:model$settings$dim$K),
                                    prevalence=model$theta%>%colMeans()) %>% arrange(prevalence)
      par(mar=c(4,6,1,1))
      barplot(topic_prevalence$prevalence, names.arg=topic_prevalence$topic,
        horiz=T, las=1, xlim=c(0,0.5), xlab="expected topic proportion")
    },
    
    plot_topic_words = function(model) {
      par(mfrow=c(1,4), mar=c(1,1,1,1))
      plot(model, type="labels", labeltype="prob", main="proba",
           cex.main=1.3, text.cex=1.3, n=15)
      plot(model, type="labels", labeltype="frex", main="frex",
           cex.main=1.3, text.cex=1.3, n=15)
      plot(model, type="labels", labeltype="lift", main="lift",
           cex.main=1.3, text.cex=1.3, n=15)
      plot(model, type="labels", labeltype="score", main="score",
           cex.main=1.3, text.cex=1.3, n=15)
    },
    
    get_top_documents = function(model){
      ft = stm::findThoughts(model, texts=self$preprocessed_data$meta$reconstructed_chars,
                        topics=1:model$settings$dim$K, n=3, meta=out$meta)
      thoughts_data = lapply(
        names(ft$index),
        function(x) data.frame(topic = rep(x, length(ft$index[x])),
          index = ft$index[x][[1]])
          ) %>%
          data.table::rbindlist() %>% as.data.frame()
      mirror_cols = c("index", "name", "type", "country", "sector", "reconstructed_text")
      thoughts_data[, mirror_cols] = self$preprocessed_data$meta[thoughts_data$index, mirror_cols]
      thoughts_data = thoughts_data[, c("index", "name", "sector", "reconstructed_text")] %>%
        mutate(reconstructed_chars=substr(reconstructed_text,1,500)) 
      return(thoughts_data)
    },
    
    get_sector_prevalence = function(model) {
      sector_prevalence = model$theta %>% as.data.frame() %>%
        mutate(sector=self$data$sector) %>% group_by(sector) %>%
        dplyr::summarise(across(everything(), mean), n=n()) %>%
        arrange(desc(n))
      K = model$settings$dim$K
      
      setnames(sector_prevalence,
        sapply(1:K, FUN=function(x) paste0("V", x)),
        sapply(1:K, FUN=function(x) paste0("Topic ", x)))
      return(sector_prevalence)
    },
    
    plot_sector_prevalence = function(sector_prevalence) {
      data = dplyr::select(sector_prevalence, contains("Topic"))
      xmax = min(c(round(max(unlist(data))+0.1,1),1))
      par(mfrow=c(8,5))
      for (i in 1:nrow(data)){
        par(mar=c(4,6,4,1))
        barplot(as.matrix(data[i, ]),
                names.ar = colnames(data), xlim=c(0,xmax),
                col="gray", horiz=T, las=1, xlab="expected topical prevalence", main=sector_prevalence$sector[i])}
    },
    
    plot_sector_tree = function(sector_prevalence){
      data = dplyr::select(sector_prevalence, contains("Topic"))
      dist_mat = philentropy::JSD(exp(as.matrix(data)), unit="log10")
      colnames(dist_mat) = sector_prevalence$sector
      rownames(dist_mat) = sector_prevalence$sector
      
      n_branch = as.integer(sqrt(nrow(dist_mat)))
      hclust = hclust(as.dist(dist_mat), method="ward.D")
      par(mar=c(5,1,1,20))
      as.dendrogram(hclust) %>%
        dendextend::set("branches_k_color",
                        value=viridis::viridis(n=n_branch), k=n_branch) %>%
        plot(xlab="tree height", cex.main=.8, cex.axis=.8, cex.lab=.8,
             horiz=T, main="", nodePar=list(lab.cex=.8, pch=NA, cex=.8))
    },
    
    
    export_artifacts = function() {
      
      model_files = list.files(self$sweep_data_dir, pattern="*.rds", full.names=T)
      
      for (f in model_files) {
        
        model = self$load_model(f)
        K = model$settings$dim$K
        subdir_name = paste0(stringi::stri_pad_left(K, width=3, pad="0"),"_topics")
        dir_path = file.path(self$export_data_dir, subdir_name)
        dir.create(dir_path, recursive=TRUE, showWarnings=FALSE)
        
        # model info
        sink(file.path(dir_path,"_model_summary.txt"))
        cat(self$get_model_info(model))
        sink()
        
        # topic prevalence
        png(filename=file.path(dir_path,"1_topic_prevalence.png"),
            width = 8, height = ceiling(K/2)+0.5, units="in", res=300)
        self$plot_topic_prevalence(model)
        dev.off()
        
        # topic words
        png(filename=file.path(dir_path,"2_topic_tokens.png"),
            width = 28, height = K, units="in", res=300)
        self$plot_topic_words(model)
        dev.off()
        
        # topic docs
        write.csv(self$get_top_documents(model),
                  file=file.path(dir_path,"3_topic_documents.csv"), row.names=FALSE)
        
        # sector prevalence
        sector_prevalence = self$get_sector_prevalence(model)
        write.csv(sector_prevalence,
                  file=file.path(dir_path,"4_sector_prevalence.csv"), row.names=FALSE)
        
        # plot sector prevalence
        png(filename=file.path(dir_path,"5_sector_prevalence.png"),
            width = 20, height = 0.5*K+7.5, units="in", res=300)
        self$plot_sector_prevalence(sector_prevalence)
        dev.off()
        
        # plot sector tree
        png(filename=file.path(dir_path,"6_sector_tree.png"),
            width = 15, height = 15, units="in", res=300)
        self$plot_sector_tree(sector_prevalence)
        dev.off()
      }
    }
  )
)    
