from abc import ABC, abstractmethod

from os import path, mkdir

from base64 import b64encode, b64decode

from datetime import datetime

from typing import Iterable

from kubernetes import client, watch
from kubernetes.client.rest import ApiException

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives import hashes

from cryptography.x509.oid import NameOID

from cryptography import x509

from contrib.utils import files

BACKEND = default_backend()


class _NoneIterator(object):
    def __iter__(self):
        return self

    def __next__(self):
        raise StopIteration


class _AssertCollectionType(object):
    def __init__(self, root_type):
        if not isinstance(root_type, type):
            raise TypeError('Expected an argument of type {} but got {}'.format(
                type,
                type(root_type)))
        self._rt = root_type
        self._stack = None
        self._trace = None

    def _check_instance(self, type_):
        if not isinstance(type_, (list, tuple, self._rt)):
            raise TypeError("Expected arguments of type {}, {} or {}".format(
                list,
                tuple,
                self._rt))
        if not isinstance(type_, self._rt):
            if self._stack is None:
                self._stack = []
                self._stack.append(iter(type_))
                # stores types for retracing steps
                self._trace = []
                self._trace.append(type(type_))
            try:
                t = self._stack[-1]
                try:
                    self._next(t)
                except StopIteration:
                    self._stack.pop()
                    self._check_instance(type_)
            except IndexError:
                # means we've finished
                self._stack = None
                self._trace = None

    def _next(self, current):
        # peek iterable stack
        n = next(current)
        self._trace.append(type(n))
        if isinstance(n, str):
            # we've reached bottom (special case to avoid iterating a string)
            self._check_inner_instance(n, self._copy_trace())
            # skip it
            itr = _NoneIterator()
        elif isinstance(n, Iterable):
            itr = iter(n)
        else:
            self._check_inner_instance(n, self._copy_trace())
            itr = _NoneIterator()
        self._stack.append(itr)
        return self._next(itr)

    def _copy_trace(self):
        f = self._trace.copy()
        self._trace.pop()
        return f

    def _repr(self, from_, initial=None):
        try:
            if not initial:
                a = from_.pop()
                b = from_.pop()
                initial = '<{}{}>'.format(b, a)
            else:
                initial = '<{}{}>'.format(from_.pop(), initial)
            return self._repr(from_, initial)
        except IndexError:
            return initial

    def _check_inner_instance(self, type_, from_):
        rt = self._rt
        if not isinstance(type_, rt):
            l = from_.copy()
            l.pop()
            self._stack = None
            self._trace = None
            raise TypeError("Expected arguments of type {} but got {}".format(
                self._repr(l, rt),
                self._repr(from_)))

    def __call__(self, func):
        def assertion(obj, value):
            self._check_instance(value)
            func(obj, value)

        return assertion


class _AssertType(object):
    def __init__(self, type_):
        if not isinstance(type_, type):
            raise TypeError('')
        self._type = type_

    def __call__(self, func):
        t = self._type

        def assertion(obj, input_):
            if t is not type(input_):
                raise TypeError('')
            func(obj, input_)

        return assertion


class CertificateSubject(object):
    def __init__(self):
        self._dns = set()
        self._attributes = {}

    @property
    def country_name(self):
        return self._get_attribute('country_name')

    @country_name.setter
    @_AssertType(str)
    def country_name(self, value):
        self._ctn = x509.NameAttribute(NameOID.COUNTRY_NAME, value)
        self._attributes['country_name'] = self._ctn

    @property
    def common_name(self):
        return self._get_attribute('common_name')

    @common_name.setter
    @_AssertType(str)
    def common_name(self, value):
        con = x509.NameAttribute(NameOID.COMMON_NAME, value)
        self._attributes['common_name'] = con

    @property
    def alternative_names(self):
        return tuple(self._dns)

    @alternative_names.setter
    @_AssertCollectionType(str)
    def alternative_names(self, values):
        for val in values:
            self._dns.add(x509.DNSName(val))

    def get_csr(self, key):
        csb = x509.CertificateSigningRequestBuilder()
        csr = csb.subject_name(x509.Name(list(self._attributes.values())))
        if self._dns:
            csr.add_extension(x509.SubjectAlternativeName(list(self._dns)),
                              critical=False)
        cert = csr.sign(key, hashes.SHA256(), BACKEND)
        # with open(path.join(storage_dir,"server.csr"),"wb") as f:
        #     f.write(c)
        return cert.public_bytes(serialization.Encoding.PEM)

    def _get_attribute(self, name):
        try:
            return self._attributes[name]
        except KeyError:
            return


class CertificateSigningRequestBuilder(object):
    def __init__(self):
        self._metadata = client.V1ObjectMeta()
        self._usages = None

    @property
    def name(self):
        return self._metadata.name

    @name.setter
    def name(self, value):
        self._metadata.name = value

    @property
    def namespace(self):
        return self._metadata.namespace

    @namespace.setter
    @_AssertType(str)
    def namespace(self, value):
        self._metadata.namespace = value

    @property
    def groups(self):
        return self._groups

    @groups.setter
    @_AssertCollectionType(str)
    def groups(self, values):
        self._groups = values

    @property
    def usages(self):
        return self._usages

    @usages.setter
    @_AssertCollectionType(str)
    def usages(self, values):
        self._usages = values

    def _get_certificate_signing_request(self, subject, private_key):
        return subject.get_csr(private_key)

    def get_certificate_signing_request(self, subject, private_key):
        body = client.V1beta1CertificateSigningRequest()
        body.api_version = 'certificates.k8s.io/v1beta1'
        body.kind = 'CertificateSigningRequest'
        body.metadata = self._metadata
        t = b64encode(
            self._get_certificate_signing_request(
                subject,
                private_key
            )
        )
        body.spec = client.V1beta1CertificateSigningRequestSpec(
            request=t.decode(),
            groups=self._groups,
            usages=self._usages
        )
        return body


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
            encryption = serialization.NoEncryption
        file.store(key.private_bytes(
            encoding=serialization.Encoding.PEM,
            format=serialization.PrivateFormat.TraditionalOpenSSL,
            encryption_algorithm=encryption
        ))

    return key


class CertificateFactory(ABC):
    def __init__(self):
        self._cert_file = None

    def _create_file(self, name):
        if not self._cert_file:
            self._cert_file = files.OverwriteByteFile(name)
        return self._cert_file

    def get_certificate(self, root_path, cert_name, key):
        try:
            return self._get_certificate(root_path, cert_name)
        except FileNotFoundError:
            certificate = b64decode(self._send_certificate_request(
                self._create_certificate(
                    root_path,
                    cert_name, key)
            ))
            self._create_file(
                path.join(
                    root_path,
                    cert_name + ".crt"
                )
            ).store(certificate)
            print('Saved certificate')
            return certificate

    # sends a certificate request to register the server
    #todo sends a request to an authority: this could be abstracted...
    def _send_certificate_request(self, body):
        try:
            api_instance = client.CertificatesV1beta1Api()
            api_instance.create_certificate_signing_request(body)
            w = watch.Watch()
            # wait for an approval event... if the request gets denied raise an error
            # if the request gets approved fetch the certificate and store it somewhere.
            print('Sending certificate signing request and waiting for a response...')
            for event in w.stream(api_instance.list_certificate_signing_request):
                obj = event['object']
                if event['type'] == 'MODIFIED':
                    condition = obj.status.conditions[-1]
                    if condition.type == 'Denied':
                        raise RuntimeError('The certificate signing request was denied.')
                    else:
                        certificate = obj.status.certificate
                        if certificate is not None:
                            print('Certificate signing request was approved!')
                            w.stop()
                            return certificate  # return certificate

        except ApiException as e:
            # catches conflict exceptions as-well.
            print("Exception when calling CertificatesV1beta1Api->create_certificate_signing_request: {}\n".format(e))

    @abstractmethod
    def _create_certificate(self, root_path, cert_name, key):
        raise NotImplementedError

    def _get_certificate(self, storage_dir, file_name):
        '''if a certificate is expired or is non-existent, a FileNotFound error is raised.'''
        try:
            cert = next(
                self._create_file(
                    path.join(
                        storage_dir,
                        file_name
                    )
                ).load()
            )
            c = x509.load_pem_x509_certificate(cert, BACKEND)
            # isinstance(c,x509.Certificate)
            validity_dt = c.not_valid_after  # check for validity of the certificate...
            # if the certificate is expired raise a file not found error?
            if datetime.utcnow() >= validity_dt:
                raise FileNotFoundError('Certificate is expired')
            return cert
        except FileNotFoundError:
            raise FileNotFoundError('No certificate')
