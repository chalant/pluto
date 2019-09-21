from abc import ABC, abstractmethod
from protos.data_bundle_pb2 import UnitDomainDef
from protos.data_bundle_pb2 import CompoundDomainDef

import pandas as pd

#TODO: add exchange to the domain struct.

class DomainBuilder(object):
    def domain(self, type, country_code, data_type):
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
    def __init__(self, country_code, data_type, asset_type):
        '''

        Parameters
        ----------
        country_code : str
        data_types : list
        set of data types we want for this domain

        Notes
        -----
        We can choose a particular domain by specifying its name, or
        '''

        self._name = country_code
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
    '''will be used to filter data packets and clock events sent by the controller'''
    __slots__ = ['_exchanges', '_data_types', 'sessions']

    def __init__(self, data_types, exchanges=None, sessions=None):
        self._exchanges = exchanges
        self._data_types = data_types
        self._sessions = sessions

    @property
    def exchanges(self):
        return self._country_codes

    @property
    def data_types(self):
        return self._data_types

    @property
    def sessions(self):
        return self._sessions


def load_domain_struct(unit_domain_def, exchange_mappings):
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
    country_code = unit_domain_def.name
    asset_type = unit_domain_def.asset_type
    return DomainStruct(set(unit_domain_def.data_type), exchange_mappings[country_code] & exchange_mappings[asset_type])

def compute_domain(dom_def, exchange_mappings, sessions_per_exchange):
    '''

    Parameters
    ----------
    compound_domain : contrib.coms.protos.data_bundle_pb2.CompoundDomainDef

    Returns
    -------
    DomainStruct

    '''
    def union(sessions_per_exchange, exchanges):
        itr = iter(exchanges)
        n0 = sessions_per_exchange[next(itr)]
        while True:
            try:
                n0 = n0.union(sessions_per_exchange[next(itr)])
            except StopIteration:
                return n0

    def apply_op(left_sessions, right_sessions):
        if op == '&':
            sessions = left_sessions.intersection(right_sessions)
        elif op == '|':
            sessions = left_sessions.union(right_sessions)
        elif op == '/':
            sessions = left_sessions.difference(right_sessions)
        elif op == '^':
            sessions = left_sessions.symmetric_difference(right_sessions)
        return sessions

    stack = []
    fc = False #first call flag
    for domain in dom_def:
        op = domain.op
        if not op:
            stack.append(load_domain_struct(domain.domain, exchange_mappings))
        else:
            left = stack.pop()
            right = stack.pop()
            if fc:
                left_sessions = union(clock_per_exchange, left.exchanges)
                fc = True
            else:
                left_sessions = left.sessions
            right_sessions = union(sessions_per_exchange, right.exchanges)
            stack.append(DomainStruct(left.data_types | right_sessions.data_types,
                                      sessions=apply_op(left_sessions, right_sessions)))

    #return the resulting domain...
    return stack.pop()

def get_exchanges(dom_def, exchange_mappings):
    exchanges = []
    for d in dom_def:
        dom = d.domain
        if not dom.op:
            exchanges.append(exchange_mappings[dom.country_code] & exchange_mappings[dom.asset_type])
    return set(exchanges)



def domain_id(dom_def):
    a = []
    for domain in dom_def:
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




