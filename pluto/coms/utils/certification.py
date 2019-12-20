from abc import ABC, abstractmethod

from os import path, mkdir

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives import hashes

from cryptography.x509.oid import NameOID

from cryptography import x509

from pluto.utils import files
from pluto.coms.utils.assertion import assert_collection_type, assert_type

BACKEND = default_backend()

class CertificateSubject(object):
    def __init__(self):
        self._names = {}

    @property
    def subject_name(self):
        try:
            return x509.Name(self._names.values())
        except AttributeError:
            raise AttributeError('No name was set')

    @property
    def country_name(self):
        return self._get_name_attribute(NameOID.COUNTRY_NAME, 'country_name')

    @country_name.setter
    def country_name(self, value):
        self._add_name(NameOID.COMMON_NAME, value)

    def _add_name(self, id_, value):
        self._names[id_] = x509.NameAttribute(id_, value)

    def _get_name_attribute(self, id_, name):
        try:
            return self._names[id_]
        except KeyError:
            raise AttributeError('{} is not set'.format(name))

    @property
    def common_name(self):
        return self._get_name_attribute(NameOID.COMMON_NAME, 'common_name')

    @common_name.setter
    def common_name(self, value):
        return self._add_name(NameOID.COMMON_NAME, value)

    @property
    def dns(self):
        try:
            return x509.SubjectAlternativeName([x509.DNSName(d) for d in self._dns])
        except AttributeError:
            raise AttributeError('dns list is not set')

    @dns.setter
    @assert_collection_type(str)
    def dns(self, value):
        self._dns = value

class CertificateSigningRequest(object):
    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        self._name = value

    @property
    def namespace(self):
        return self._namespace

    @namespace.setter
    def namespace(self, value):
        self._namespace = value

    @property
    def groups(self):
        return self._groups

    @groups.setter
    @assert_collection_type(str)
    def groups(self, values):
        self._groups = values

    @property
    def usages(self):
        return self._usages

    @usages.setter
    @assert_collection_type(str)
    def usages(self, values):
        self._usages = values

    @property
    def certificate(self):
        return self._certificate

    @certificate.setter
    @assert_type(bytes)
    def certificate(self, value):
        self._certificate = value


class CertificateSigning(ABC):
    @abstractmethod
    def sign_certificate(self, request):
        raise NotImplementedError


def create_csr(key, subject):
    """

    Parameters
    ----------
    key
    subject : CertificateSubject

    Returns
    -------

    """
    csb = x509.CertificateSigningRequestBuilder()
    # todo: is setting a name necessary?
    csr = csb.subject_name(subject.subject_name)
    try:
        csr.add_extension(subject.dns)
    except AttributeError:
        pass

    cert = csr.sign(key, hashes.SHA256(), BACKEND)
    # with open(path.join(storage_dir,"server.csr"),"wb") as f:
    #     f.write(c)
    return cert.public_bytes(serialization.Encoding.PEM)


def _ensure_dir(storage_dir):
    if not path.exists(storage_dir):
        mkdir(storage_dir)


def get_key(storage_dir, key_file_name=None, password=None):
    if not key_file_name:
        path_to_key = path.join(storage_dir, "key.pem")
    else:
        path_to_key = path.join(storage_dir, key_file_name + '.pem')

    file = files.OverwriteByteFile(path_to_key)

    if path.exists(path_to_key):
        key = serialization.load_pem_private_key(
            next(file.load()),
            password.encode(),
            BACKEND
        )
    else:
        key = rsa.generate_private_key(
            public_exponent=65537,
            key_size=2048,
            backend=BACKEND
        )
        if password:
            encryption = serialization.BestAvailableEncryption(password.encode())
        else:
            encryption = serialization.NoEncryption()
        file.store(key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=encryption
        ))

    return key


def load_certificate(path):
    with open(path, 'rb') as f:
        return x509.load_pem_x509_certificate(f.read(), BACKEND)


def certificate_subject_parser():
    pass