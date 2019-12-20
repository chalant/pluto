from .certification import (
    CertificateSubject,
    CertificateSigningRequestBuilder,
    send_certificate_request,
    get_key)

from kubernetes import config

config.load_kube_config()

DIR = '/home/yves/test_certification'
PASSWORD = 'blablabla'

key = get_key(DIR, key_file_name='test', password=PASSWORD)
subject = CertificateSubject()
subject.common_name = "server1.default.svc.cluster.local"
subject.alternative_names = ["server2.default.svc.cluster.local"]

builder = CertificateSigningRequestBuilder()
builder.name = 'controller'
builder.usages = ['digital signature',
                  'key encipherment',
                  'data encipherment',
                  'server auth']
builder.groups = ['system: authenticated']

cert = send_certificate_request(DIR, subject, 'test', key, builder)
