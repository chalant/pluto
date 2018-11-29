def reformat_symbol(symbol):
	if not isinstance(symbol,str):
		raise TypeError
	return symbol.replace('-','.')