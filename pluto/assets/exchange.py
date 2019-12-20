class Exchange(object):
    def __init__(self, name, asset_types, country_code):
        '''

        Parameters
        ----------
        name : str
        asset_types : list
        country_code : str
        '''
        self._name = name
        self._asset_types = asset_types
        self._country_code = country_code

