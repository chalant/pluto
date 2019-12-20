from abc import ABC, abstractmethod
from protos.data_bundle_pb2 import UnitDomainDef
from protos.data_bundle_pb2 import CompoundDomainDef

import pandas as pd


class DomainBuilder(object):
    def country_code(self, name):
        '''

        Parameters
        ----------
        name : str

        Returns
        -------
        BaseDomain
        '''
        return BaseDomain(name)

    def union(self, dom1, dom2):
        '''

        Parameters
        ----------
        dom1 : Domain
        dom2 : Domain

        Returns
        -------
        CompoundDomain

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
        CompoundDomain
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
        CompoundDomain
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
        CompoundDomain
        '''
        return CompoundDomain(dom1, dom2, '^')


class Domain(ABC):
    @abstractmethod
    def compile(self):
        raise NotImplementedError


class BaseDomain(Domain):
    __slots__ = ['_name']

    def __init__(self, country_code):
        '''

        Parameters
        ----------
        country_code : str
        '''

        self._name = country_code

    def compile(self):
        return [UnitDomainDef(name=self._name)]


class CompoundDomain(Domain):
    __slots__ = ['_root', '_left', '_right']

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
    __slots__ = ['_exchanges', '_sessions']

    def __init__(self, exchanges, sessions):
        self._exchanges = exchanges
        self._sessions = sessions

    @property
    def exchanges(self):
        return self._exchanges

    @property
    def sessions(self):
        return self._sessions


def load_domain_struct(unit_domain_def, exchange_per_country_code, sessions_per_exchange):
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
    exchanges = exchange_per_country_code[country_code]
    sessions = None
    for exchange in exchanges:
        if not sessions:
            sessions = sessions_per_exchange[exchange]
        else:
            sessions = sessions.union(sessions)
    return DomainStruct(exchanges=exchanges, sessions=sessions)


def compute_domain(dom_def, exchange_per_country_code, sessions_per_exchange):
    '''

    Parameters
    ----------
    compound_domain : contrib.coms.protos.data_bundle_pb2.CompoundDomainDef

    Returns
    -------
    DomainStruct

    '''

    def apply_op(left_dom_struct, right_dom_struct):
        left_sessions = left_dom_struct.sessions
        right_sessions = right_dom_struct.sessions
        left_exchanges = left_dom_struct.exchanges
        right_exchanges = right_dom_struct.exchanges

        if op == '&':
            exchanges = left_exchanges | right_exchanges
            sessions = left_sessions.intersection(right_sessions)
        elif op == '|':
            exchanges = left_exchanges | right_exchanges
            sessions = left_sessions.union(right_sessions)
        elif op == '/':
            exchanges = left_exchanges / right_exchanges
            sessions = left_sessions.difference(right_sessions)
        elif op == '^':
            exchanges = left_exchanges | right_exchanges
            sessions = left_sessions.symmetric_difference(right_sessions)
        return DomainStruct(exchanges=exchanges, sessions=sessions)

    stack = []
    for domain in dom_def.domains:
        op = domain.op
        if not op:
            stack.append(load_domain_struct(
                domain.domain,
                exchange_per_country_code,
                sessions_per_exchange))
        else:
            stack.append(apply_op(stack.pop(), stack.pop()))
    # return the resulting domain...
    return stack.pop()


def get_exchanges(dom_def, exchange_per_country_code):
    exchanges = []
    for d in dom_def.domains:
        dom = d.domain
        if not dom.op:
            exchanges.extend(exchange_per_country_code[dom.country_code])
    return set(exchanges)


def domain_id(dom_def):
    a = []
    for domain in dom_def:
        op = domain.op
        if not op:
            a.append(domain.name)
        else:
            a.append(op)
    return hash(tuple(a))


def compile(domain):
    return CompoundDomainDef(domains=domain.compile())


'''
example: this is how to build a domain of equities and futures that are traded in the 
US and that have fundamental data.

builder = DomainBuilder()

US = builder.domain('US')
BE = builder.domain('BE')
FR = builder.domain('FR')

US_AND_BE = builder.intersection(US, BE)
US_OR_BE = builder.union(US, BE)
US_NOT_FR = builder.difference(US, FR)

US_NOT_FR_AND_BE = builder.union(US_NOT_FR, BE)

'''
