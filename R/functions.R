library(lubridate)
library(data.table)
library(iterators)
library(foreach)
tryCatch({library(doMC)}, error =function(e) {library(doParallel)})

fullRank = function(x) {
	z = qr(x)
	return(x[,z$pivot[1:z$rank]])
}

minmaxNorm=function(x,newmin,newmax,floorNA=FALSE) {
	if(floorNA)
		x[is.na(x)] = min(x,na.rm=TRUE)
	return(  (x-min(x))/(max(x)-min(x)) * (newmax-newmin) + newmin  )
}

'%nin%' = function(x,y) {
  return(!(x %in% y))
}