from abc import ABC, abstractmethod
from protos import UnitDomainDef
from protos import CompoundDomainDef

class DomainBuilder(object):
    def asset(self, type, country_code, data_type):
        return BaseDomain(country_code, data_type, type)

    def union(self, dom1, dom2):
        '''

        Parameters
        ----------
        dom1 : Domain
        dom2 : Domain

        Returns
        -------

        '''
        return CompoundDomain(dom1, dom2, '|')

    def intersection(self, dom1, dom2):
        '''

        Parameters
        ----------
        dom1 : Domain
        dom2 : Domain

        Returns
        -------

        '''
        return CompoundDomain(dom1, dom2, '&')

    def difference(self, dom1, dom2):
        '''

        Parameters
        ----------
        dom1 : Domain
        dom2 : Domain

        Returns
        -------

        '''
        return CompoundDomain(dom1, dom2, '/')

    def symmetric_difference(self, dom1, dom2):
        '''

        Parameters
        ----------
        dom1 : Domain
        dom2 : Domain

        Returns
        -------

        '''
        return CompoundDomain(dom1, dom2, '^')

class Domain(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def compile(self):
        raise NotImplementedError

class BaseDomain(Domain):
    def __init__(self, name, data_type, asset_type):
        '''

        Parameters
        ----------
        name : str
        data_types : list
        set of data types we want for this domain

        Notes
        -----
        We can choose a particular domain by specifying its name, or
        '''

        self._name = name
        self._data_type = data_type
        self._asset_type = asset_type

    def compile(self):
        return [UnitDomainDef(name=self._name, asset_type = self._asset_type, data_type = self._data_type)]

class CompoundDomain(Domain):
    def __init__(self, left, right, operator):
        '''

        Parameters
        ----------
        left : Domain
        right : Domain
        operator : str
        '''
        self._root = operator
        self._left = left
        self._right = right

    def compile(self):
        a = []
        a.extend(self._left.compile())
        a.extend(self._right.compile())
        a.append(self._root)
        return a

class DomainStruct(object):
    '''will be used to filter data packets sent by the controller'''
    __slots__ = ['_sessions', '_country_codes', '_data_types', '_asset_types']

    def __init__(self, sessions, country_codes, data_types, asset_types):
        self._sessions = sessions
        self._country_codes = country_codes
        self._data_types = data_types
        self._asset_types = asset_types

    @property
    def sessions(self):
        return self._sessions

    @property
    def country_codes(self):
        return self._country_codes

    @property
    def data_types(self):
        return self._data_types

    @property
    def asset_types(self):
        return self._asset_types

def apply_operator(left, right, operator):
    '''

    Parameters
    ----------
    left : DomainStruct
    right : DomainStruct
    operator : str

    Returns
    -------
    DomainStruct
    '''
    if operator == '|':
        return
    elif operator == '&':
        return
    elif operator == '/':
        return
    elif operator == '^':
        return
    else:
        raise ValueError('')

def load_domain_struct(unit_domain_def):
    '''
    Parameters
    ----------
    unit_domain_def : contrib.coms.protos.data_bundle_pb2.UnitDomainDef

    Returns
    -------
    DomainStruct

    Notes
    -----
    Loads a DomainStruct object from the unit domain def
    '''
    return

def compute_domain(dom_def):
    '''

    Parameters
    ----------
    compound_domain : contrib.coms.protos.data_bundle_pb2.CompoundDomainDef

    Returns
    -------
    DomainStruct
    '''
    stack = []
    for domain in dom_def:
        op = domain.op
        if not op:
            stack.append(load_domain_struct(domain.domain))
        else:
            stack.append(apply_operator(stack.pop(),stack.pop(),op))

    #return the resulting domain...
    return stack.pop()

def domain_id(domains):
    a = []
    for domain in domains:
        op = domain.op
        if not op:
            a.append((domain.name, domain.asset_type, domain.data_type))
        else:
            a.append(op)
    return hash(tuple(a))


def compile(domain):
    return CompoundDomainDef(domains = domain.compile())

'''
example: this is how to build a domain of equities and futures that are traded in the 
US and that have fundamental data.

builder = DomainBuilder()

FULL_US_DOMAIN = builder.union(
    builder.asset('US', 'equity', 'fundamental'), 
    builder.asset('US', 'futures', 'fundamental')
)'''




