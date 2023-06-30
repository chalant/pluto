class AssetsDB(object):
	def set_corporate_actions(self, corporate_actions):
		pass

	def get_assets(self, **kwargs):
		pass

	def set_as_delisted(assets):
		pass

	def add_assets(assets):
		pass

# todo: the ingestion db is an interface to our ingetion state data 
# (might be a combination of sqlite and click-house...)
def IngestionDB(object):
	def __init__(self):
		self._assets_db = AssetsDB()
	
	def get_asset_db(calendar_name):
		return 

	def get_assets(calendar_name):
		pass
	