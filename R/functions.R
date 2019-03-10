require(lubridate)
require(data.table)
require(iterators)
require(foreach)
require(doMC)

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