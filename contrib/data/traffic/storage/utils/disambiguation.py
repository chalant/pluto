'''module for maintaining non-ambiguous symbols... keeps track of changed tickers etc.'''

from contrib.data.traffic.storage.database.mongo_utils import get_collection

_META_DATA = get_collection('AvailableTickers')
_TICKERS = get_collection('Tickers')

def _resolve_symbol_conflicts(self,show_progress=True):
	with tqdm.tqdm(total=_META_DATA.count()) as progress:
		for data in _META_DATA.find({}):
			ticker = data['Ticker']
			if '-' in ticker or '.' in ticker:
				d = _TICKERS.find({'Related Tickers': ticker.replace('-', '.')})
				for c in d:
					exchange = c['Exchange']
					if exchange == 'DELISTED':
						exchange = c['Delisted From']
						delisted = True
					else:
						delisted = False
					n1 = data['Name']
					t = c['Ticker']
					if '.' in ticker:
						_META_DATA.update_one({'Ticker': ticker},
											  {'$set': {'Ticker': ticker.replace('.', '-'),
														'Exchange': exchange}})
					t0 = ticker.replace('-', '').replace('.', '')
					if t0 != t:
						related_tickers = [i.replace('.', '') for i in c['Related Tickers']]
						t = related_tickers[related_tickers.index(t0)]
					if n1:
						if self._resolve_name(n1, c['Name']):
							progress.set_description('Processing {} {}'.format(ticker, t))
							for m in _META_DATA.find({'Ticker': t}):
								n2 = m['Name']
								if n2:
									if self._resolve_name(n1, n2):
										# update with the rest...
										_META_DATA.update_one({'Ticker': ticker},
															  {'$set': {'Name': n1,
																		'Delisted': delisted,
																		'Exchange': exchange}})
										_DAILY_EQUITY_DATA.update_one({'Ticker': t},
																	  {'$set': {'Ticker': ticker}})
										break
								else:
									_META_DATA.update_one({'Ticker': ticker},
														  {'$set': {'Name': n1,
																	'Delisted': delisted,
																	'Exchange': exchange}})
									_DAILY_EQUITY_DATA.update_one({'Ticker': t},
																  {'$set': {'Ticker': ticker}})
									break
					else:
						_META_DATA.update_one({'Ticker': ticker},
											  {'$set': {'Name': c['Name'],
														'Delisted': delisted,
														'Exchange': exchange}})
						_DAILY_EQUITY_DATA.update_one({'Ticker': t},
													  {'$set': {'Ticker': ticker}})
						break
			progress.update(1)

def resolve_symbol(symbol, name):  # type (future, equity, ...)
	'''returns non-conflicting symbol... but we might have some ambiguous results'''
	if not name:
		doc = _META_DATA.find_one({'Ticker': symbol})
		if doc:
			return doc['Ticker']
		else:
			doc = _TICKERS.find({'Related Tickers': symbol})
			if doc.count():
				assets = []
				for d in doc:
					ticker = d['Ticker']
					dta = _META_DATA.find_one({'Ticker': ticker})
					if dta:
						assets.append(_create_asset(dta))
				return assets
	else:
		# FIXME: fix this part...
		return _find_by_name(symbol, name)

	perfect_matches = []
	probable_matches = []
	found = {}
	probably_found = []

	def add_found(searched, found_):
		if searched in found:
			found[searched].append(found_)
		else:
			a = []
			a.append(found_)
			found[searched] = a

	def search_related_tickers(symbol):
		docs = _TICKERS.find({'Related Tickers': symbol})
		if docs.count():
			for doc in docs:
				exchange = doc['Exchange']
				if exchange == 'DELISTED':
					exchange = doc['Delisted From']
					delisted = True
				else:
					delisted = False
				name_ = doc['Name']
				ticker = doc['Ticker']
				prior_tickers = doc['Prior Tickers']
				if self._resolve_name(name, name_):
					if delisted:
						perfect_matches.append(ticker)
						add_found(symbol, Asset(name_, exchange, ticker, delisted, None, None, None))
					elif prior_tickers:
						perfect_matches.append(ticker)
						add_found(symbol, Asset(name_, exchange, ticker, delisted, None, None, None))
					else:
						related_tickers = doc['Related Tickers']
						if related_tickers:
							related_tickers = [i.replace('.', '') for i in related_tickers]
							if len(related_tickers) > 1:
								ticker = related_tickers[related_tickers.index(symbol)]
							else:
								ticker = related_tickers[0]
							perfect_matches.append(ticker)
							add_found(symbol, Asset(name_, exchange, ticker, delisted, None,
													None, None))
				else:
					if prior_tickers:
						if symbol in prior_tickers:
							probable_matches.append(ticker)
							probably_found.append(symbol)

	doc = _TICKERS.find_one({'Ticker': symbol})
	if doc:
		ticker = doc['Ticker']
		name_ = doc['Name']
		if _resolve_name(name, name_):
			perfect_matches.append(ticker)
			add_found(symbol, ticker)
		else:
			search_related_tickers(symbol)
	else:
		search_related_tickers(symbol)
	for pr, s in zip(probable_matches, probably_found):
		if s not in found:
			perfect_matches.append(pr)
			add_found(s, pr)
	results = []
	for value in found.values():
		# find unique symbol...
		if len(value) > 1:
			for v in value:
				doc = _daily_equity_data.find_one({'Ticker': v.symbol}, projection={'Series': 0})
				if doc:
					results.append(v)
					break
		else:
			results.append(value[0])
	return results

def _clean_name(name):
	if '(' and ')' in name:
		name = name[0:name.index('(')] + name[name.index(')') + 1:]
	return name.replace('!', '').replace("'", '') \
		.replace(',', '').replace('.', '').replace('*', ' ').replace('-', ' ').replace('&', '')

def _resolve_name(name1, name2):
	name1 =_clean_name(name1)
	name2 = _clean_name(name2)
	if name1 in name2 or name2 in name1:
		return True
	else:
		# check by word...
		arr1 = [word for word in name1.split(' ') if word and word != 'CORP' and word != 'INC']
		arr2 = [word for word in name2.split(' ') if word and word != 'CORP' and word != 'INC']
		l1 = len(arr1)
		l2 = len(arr2)
		if l1 < l2:
			counter = 0
			for i in range(l1):
				if arr1[i] in arr2:
					counter += 1
			if counter >= 1:
				return True
			else:
				abv = ''.join(arr2)
				for word in arr1:
					if word in abv:
						return True
				else:
					abv = ''.join([word[0] for word in arr2 if word != 'CO'])
					for word in arr1:
						if 'CO' in word:
							word = word.replace('CO', '')
						if abv == word:
							return True
					else:
						return False
		elif l2 < l1:
			counter = 0
			for i in range(l2):
				if arr2[i] in arr1:
					counter += 1
			if counter >= 1:
				return True
			else:
				abv = ''.join(arr1)  # check if the word is split...
				for word in arr2:
					if word in abv:
						return True
				else:
					abv = ''.join(
						[word[0] for word in arr1 if word != 'CO'])  # check if the word is abbreviated
					for word in arr2:
						if 'CO' in word:
							word = word.replace('CO', '')
						if abv == word:
							return True
					else:
						return False

		elif l1 == l2:
			for i in range(l1):
				word = arr1[i]
				if word in arr2 and len(word) > 1:
					return True
			else:
				return False
		else:
			return False

'''equities metadata should be: symbol,name,exchange,start_date,end_date'''