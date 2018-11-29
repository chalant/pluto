from .executor import _RequestExecutor
from contrib.data.download import SandPConstituents
import requests
from bs4 import BeautifulSoup
from dateutil.parser import parse


class WikipediaExecutor(_RequestExecutor):
	url = "http://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

	def _execute(self, request):
		if isinstance(request,SandPConstituents):
			page = requests.get(self.url)
			soup = BeautifulSoup(page.content, "html.parser")
			return self._extract_table(soup.find_all('table', {'class': 'wikitable sortable'})[0])
		else:
			return None

	def _cool_down_time(self):
		return 86400

	# TODO: should probably add regex to find some keys like date, datetime etc...
	def _extract_table(self, table):
		new_table = {}
		self._align_rows(iter(table.find_all('tr')), new_table)
		return self._create_document(new_table)

	def _create_document(self, table):
		def compress(doc, i=0):
			try:
				doc1 = doc[i]
				doc2 = doc[i + 1]
				if doc1[0] == doc2[0]:
					doc[i] = doc1[0:] + doc2[1:]
					doc.pop(i + 1)
				else:
					i += 1
				return compress(doc, i)
			except IndexError:
				return doc

		def fold(doc):
			a = {}
			for d in doc:
				d = list(d)
				l = len(d)
				if l % 2 != 0:
					head = d.pop(0)
					l -= 1
				else:
					head = None
				fields = [d[j] for j in range(l) if j % 2 != 0]
				tags = [d[j] for j in range(l) if j % 2 == 0]
				for tag, field in zip(tags, fields):
					if 'Date' in tag:
						if field:
							field = parse(field)
					if head:
						if head in a:
							dct = a[head]
						else:
							dct = {}
							a[head] = dct
						dct[tag] = field
					else:
						a[tag] = field
			return a

		headers = table['headers']
		columns = table['columns']
		lh = len(headers)
		# align headers
		for i in range(lh - 1):
			h1 = headers[i]
			h2 = headers[i + 1]
			lh1 = len(h1)
			lh2 = len(h2)
			if lh1 < lh2:
				for _ in range(lh2 - lh1):
					h1.append(h1[-1])
			elif lh1 > lh2:
				for _ in range(lh1 - lh2):
					h2.append(h2[-1])
		return [fold(compress(list(zip(*headers, column)))) for column in columns]

	def _align_rows(self, iterator, array):
		try:
			row = next(iterator)
			header = row.find_all('th')
			if header:
				extension = []
				for h in header:
					try:
						i = int(h['colspan'])
						for k in range(i):
							extension.append(h.text)
					except (ValueError, KeyError):
						extension.append(h.text)
				if 'headers' in array:
					headers = array['headers']
				else:
					headers = []
					array['headers'] = headers
				headers.append(extension)
			else:
				if 'columns' in array:
					columns = array['columns']
				else:
					columns = []
					array['columns'] = columns
				col = row.find_all('td')
				extension = []
				flag = False
				r_fl = False
				c_row = []
				for c in col:
					try:
						j = int(c['rowspan'])
						if not flag:
							for _ in range(j):
								try:
									n_cols = []
									n_cols.append(c.text)
									if not r_fl:
										n_cols.extend([t.text for t in col[1:len(col) - 1]])
										r_fl = True
									else:
										n_cols.extend([t.text for t in next(iterator).find_all('td')])
									extension.append(n_cols)
								except StopIteration:
									pass
							flag = True
						else:
							for e in extension:
								e.append(c.text)
					except (ValueError, KeyError):
						if not flag:
							c_row.append(c.text)
				if c_row:
					extension.append(c_row)
				columns.extend(extension)
			self._align_rows(iterator, array)
		except StopIteration:
			pass
