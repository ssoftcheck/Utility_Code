library(data.table)
library(hashmap)
library(progress)
library(parallel)

lookup = function(vars,xdata,yvar,wvar=NULL,threads=1) {
  result = vector(mode='list',length = length(vars))
  names(result) = vars
  
  if(threads == 1) {
    if(is.null(wvar)) {
      pb = progress_bar$new(format=' [:bar] :percent Time Left: :eta',total=length(vars),clear=FALSE)
      for(v in vars) {
        result[[v]] = xdata[!is.na(get(yvar)),.(value=sum(get(yvar)),weight=.N),by=v]
        pb$tick()
      }
      result$popMean = mean(xdata[!is.na(get(yvar)),get(yvar)])
    }
    else {
      if(any(is.na(xdata[[wvar]]))) {
        message('Negative Weights')
        stop()
      }
      pb = progress_bar$new(format=' [:bar] :percent Time Left: :eta',total=length(vars),clear=FALSE)
      for(v in vars) {
        result[[v]] = xdata[!is.na(get(yvar)),.(value=sum(get(yvar) * get(wvar)),weight=sum(get(wvar))),by=v]
        pb$tick()
      }
      result$popMean = weighted.mean(xdata[!is.na(get(yvar)),get(yvar)],xdata[!is.na(get(yvar)),get(wvar)])
    }
  }
  # multi thread/core
  else {
    if(is.null(wvar)) {
      result = mclapply(vars,function(v) xdata[!is.na(get(yvar)),.(value=sum(get(yvar)),weight=.N),by=v], mc.cores=threads)
      names(result) = vars
      result$popMean = mean(xdata[!is.na(get(yvar)),get(yvar)])
    }
    else {
      if(any(is.na(xdata[[wvar]]))) {
        message('Negative Weights')
        stop()
      }
      result = mclapply(vars,function(v) xdata[!is.na(get(yvar)),.(value=sum(get(yvar) * get(wvar)),weight=sum(get(wvar))),by=v], mc.cores=threads)
      names(result) = vars
      result$popMean = weighted.mean(xdata[!is.na(get(yvar)),get(yvar)],xdata[!is.na(get(yvar)),get(wvar)])
    }  
  }
  return(result)
}

applyLookup = function(xdata,input,yvar,train=NULL,trainvalue=NULL,wvar=NULL,jitter=1,meanWeight=1,useHash=FALSE,threads=1) {
  vars = setdiff(names(input),'popMean')
  
  result = vector(mode='list',length = length(vars))
  names(result) = vars
  
  if(!is.null(train)) {
    isTrain = xdata[,get(train) %in% trainvalue]
  } else {
    isTrain = rep(FALSE, nrow(xdata))
  }
  
  if(threads == 1) {
    pb = progress_bar$new(format=' [:bar] :percent Time Left: :eta',total=length(vars),clear=FALSE)
    if(is.null(wvar)) {
      for(v in vars) {
        temp = xdata[,c(v,yvar),with=FALSE]
        # hash merge
        if(useHash) {
          hv = hashmap(input[[v]][[v]],values = input[[v]]$value)
          hw = hashmap(input[[v]][[v]],values = input[[v]]$weight)
          temp[,c("value", "weight") := .(hv$find(temp[[v]]),hw$find(temp[[v]]))]
          hv$clear()
          hw$clear()
          rm(hv,hw)
        }
        else {
          temp[input[[v]],on=v, c("value", "weight") := mget(paste0("i.",c("value", "weight")))]
        }
        temp[,calc := (value - ifelse(isTrain,get(yvar),0) + ifelse(isTrain,meanWeight * input$popMean,0)) / ifelse(isTrain,(weight - 1 + meanWeight),weight)]
        temp[is.na(calc),calc := input$popMean]
        temp[isTrain, calc := jitter(calc,amount=jitter)]
        result[[v]] = temp$calc
        
        rm(temp)
        pb$tick()
      }
    }
    else {
      if(any(is.na(xdata[[wvar]]))) {
        message('Negative Weights')
        stop()
      }
      for(v in vars) {
        temp = xdata[,c(v,yvar,wvar),with=FALSE]
        # hash merge
        if(useHash) {
          hv = hashmap(input[[v]][[v]],values = input[[v]]$value)
          hw = hashmap(input[[v]][[v]],values = input[[v]]$weight)
          temp[,c("value", "weight") := .(hv$find(temp[[v]]), hw$find(temp[[v]]))]
          hv$clear()
          hw$clear()
          rm(hv,hw)
        }
        else {
          temp[input[[v]], on=v, c("value", "weight") := mget(paste0("i.",c("value", "weight")))]
        }
        temp[,calc := (value - ifelse(isTrain,get(yvar)*get(wvar),0) + ifelse(isTrain,meanWeight * input$popMean,0)) / ifelse(isTrain,(weight - get(wvar) + meanWeight),weight)]
        temp[is.na(calc),calc := input$popMean]
        temp[isTrain, calc := jitter(calc,amount=jitter)]
        result[[v]] = temp$calc
        
        rm(temp)
        pb$tick()
      }
    }
  }
  # multi thread/core
  else {
    if(is.null(wvar)) {
      result = mclapply(vars,function(v) {
        temp = xdata[,c(v,yvar),with=FALSE]
        if(useHash) {
          hv = hashmap(input[[v]][[v]],values = input[[v]]$value)
          hw = hashmap(input[[v]][[v]],values = input[[v]]$weight)
          temp[,c("value", "weight") := .(hv$find(temp[[v]]),hw$find(temp[[v]]))]
          hv$clear()
          hw$clear()
          rm(hv,hw)
        }
        else {
          temp[input[[v]],on=v, c("value", "weight") := mget(paste0("i.",c("value", "weight")))]
        }
        temp[,calc := (value - ifelse(isTrain,get(yvar),0) + ifelse(isTrain,meanWeight * input$popMean,0)) / ifelse(isTrain,(weight - 1 + meanWeight),weight)]
        temp[is.na(calc),calc := input$popMean]
        temp[isTrain, calc := jitter(calc,amount=jitter)]
        return(temp$calc)
      }, mc.cores=threads)
      names(result) = vars
    }
    else {
      if(any(is.na(xdata[[wvar]]))) {
        message('Negative Weights')
        stop()
      }
      result = mclapply(vars, function(v) {
        temp = xdata[,c(v,yvar,wvar),with=FALSE]
        # hash merge
        if(useHash) {
          hv = hashmap(input[[v]][[v]],values = input[[v]]$value)
          hw = hashmap(input[[v]][[v]],values = input[[v]]$weight)
          temp[,c("value", "weight") := .(hv$find(temp[[v]]), hw$find(temp[[v]]))]
          hv$clear()
          hw$clear()
          rm(hv,hw)
        }
        else {
          temp[input[[v]], on=v, c("value", "weight") := mget(paste0("i.",c("value", "weight")))]
        }
        temp[,calc := (value - ifelse(isTrain,get(yvar)*get(wvar),0) + ifelse(isTrain,meanWeight * input$popMean,0)) / ifelse(isTrain,(weight - get(wvar) + meanWeight),weight)]
        temp[is.na(calc),calc := input$popMean]
        temp[isTrain, calc := jitter(calc,amount=jitter)]
        return(temp$calc)
      })
      names(result) = vars
    }
  }
  result = as.data.table(result)
  setnames(result,names(result),paste('meancode',yvar,names(result),sep='.'))
  return(result)
}