# custom is NULL or a 2D list. Each element of custom is a list with 2 elements:
# element 1 is a single string of a primitive R type e.g. numeric, factor, integer, character
# element 2 is a vector with named functions. The functions will be applied to all types in element 1 and the column result will be named with the given name
# E.G. if you want the mean of absolute values for some reason: custom=list(list('numeric',c(max_abs=function(x) mean(abs(x)))))

generateSummary = function(indata,quantiles=c(0.5),missing_string=c('','NA','NULL'),custom=NULL, threads=1) {
  col_classes = split(names(indata),sapply(indata,function(x) paste(class(x),collapse = "_")))
  numcols = unlist(col_classes[c('numeric','integer','integer64')])
  datecols = unlist(col_classes[c('Date','POSIXct_POSIXt')])
  charcols = unlist(col_classes[c('character','factor')])
  boolcols = unlist(col_classes['logical'])
  
  if(!is.null(numcols)) {
    numstats = indata[,mclapply(.SD,function(x) c(
      sum(is.na(x)),
      sum(!is.na(x)),
      length(unique(na.omit(x))),
      mean(x,na.rm = TRUE),
      sd(x,na.rm = TRUE),
      min(x,na.rm = TRUE),
      max(x,na.rm = TRUE),
      quantile(x,probs = quantiles,na.rm = TRUE)
      ),mc.cores=threads),.SDcols=numcols]
    numstats = cbind(variable=numcols,as.data.table(t(numstats)))
    setnames(numstats,names(numstats)[-1],c('missing','non_missing','distinct','avg','sd','min','max',paste0('q',quantiles*100)))
  } 
  else {
    numstats = NULL
  }
  
  if(!is.null(datecols)) {
    datestats = indata[,mclapply(.SD,function(x) c(
      sum(is.na(x)),
      sum(!is.na(x)),
      length(unique(na.omit(x))),
      as.numeric(mean(x,na.rm = TRUE)),
      sd(x,na.rm = TRUE),
      as.numeric(min(x,na.rm = TRUE)),
      as.numeric(max(x,na.rm = TRUE)),
      quantile(as.numeric(x),probs = quantiles,na.rm = TRUE)
    ),mc.cores=threads),.SDcols=datecols]
    datestats = cbind(variable=datecols,as.data.table(t(datestats)))
    setnames(datestats,names(datestats)[-1],c('missing','non_missing','distinct','avg','sd','min','max',paste0('q',quantiles*100)))
  } 
  else {
    datestats = NULL
  }
  
  if(!is.null(boolcols)) {
    boolstats = indata[,mclapply(.SD,function(x) c(
      sum(is.na(x)),
      sum(!is.na(x)),
      length(unique(na.omit(x))),
      mean(x,na.rm=TRUE),
      sd(x,na.rm=TRUE)
      ),mc.cores=threads),.SDcols=boolcols]
    boolstats = cbind(variable=boolcols,as.data.table(t(boolstats)))
    setnames(boolstats,names(boolstats)[-1],c('missing','non_missing','distinct','avg','sd'))
  }
  else {
    boolstats = NULL
  }
  
  if(!is.null(charcols)) {
    charstats = indata[,mclapply(.SD,function(x) c(
      sum(is.na(x) | as.character(x) %in% missing_string),
      sum(!(is.na(x) | as.character(x) %in% missing_string)),
      length(unique(na.omit(x)))
    ),mc.cores=threads),.SDcols=charcols]
    charstats = cbind(variable=charcols,as.data.table(t(charstats)))
    setnames(charstats,names(charstats)[-1],c('missing','non_missing','distinct'))
  }
  else {
    charstats = NULL
  }
  
  final = Reduce(function(x,y) rbind(x,y,fill=TRUE),list(numstats,datestats,boolstats,charstats)[!sapply(list(numstats,datestats,boolstats,charstats),is.null)])
  
  if(!is.null(custom)) {
    customstats = Reduce(function(x,y) rbind(x,y,fill=TRUE),
           Map(function(x) 
             Reduce(merge,
                    lapply(names(x[[2]]),function(y) 
                      applyAndRename(indata,unlist(col_classes[x[[1]]]),x[[2]][[y]],y))),custom))
    final = merge(final,customstats,by='variable',all.x=TRUE,all.y=TRUE)
  }
  final = final[order(variable)]
  final[,type := sapply(indata[,final$variable,with=FALSE],function(x) paste(class(x),collapse="_"))]
  if('missing' %in% names(final)) {
    final[,missing_pct := 100*missing/(missing+non_missing)]
  }

  return(final)
}