require(fftw)

fftAutoCov = function(signal) {
	signal = signal - mean(signal)
	L = length(signal)
	#zero-padding
	zeros = L/2
	signal = c( rep(0,zeros), signal, rep(0,zeros) )
	f = Re(IFFT(abs(FFT(signal))**2))

	correction = c( rep(1,L), rep(0,L) )
	correction = Re(IFFT(abs(FFT(correction))**2))

	return( f[1:L]/correction[1:L])
}

fftCrossCov = function(s1,s2) {
	s1 = s1 - mean(s1)
	s2 = s2 - mean(s2)
	L1 = length(s1)
	L2 = length(s2)

	#zero-padding and extension
	if(L2 > L1)
	{
		zeros = L2/2
		s2 = c( rep(0,zeros), s2, rep(0,zeros) )
		zeros = (length(s2) - length(s1))/2
		s1 = c( rep(0,zeros), s1, rep(0,zeros))
	}
	else if(L2 < L1)
	{
		zeros = L1/2
		s1 = c( rep(0,zeros), s1, rep(0,zeros) )
		zeros = (length(s1) - length(s2))/2
		s2 = c( rep(0,zeros), s2, rep(0,zeros))
	}
	else
	{
		zeros = L1/2
		s1 = c( rep(0,zeros), s1, rep(0,zeros) )
		s2 = c( rep(0,zeros), s2, rep(0,zeros) )
	}

	f = Re(IFFT( FFT(s1) * Conj(FFT(s2)) ))
	correction = c( rep(1,max(L1,L2)), rep(0,max(L1,L2)) )
	correction = Re(IFFT(abs(FFT(correction))**2))
	return( f[1:L2]/correction[1:L2] )
}
