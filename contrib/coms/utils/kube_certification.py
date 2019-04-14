"""
A kubernetes implementation of the certification service.
"""
from base64 import b64encode

from contrib.coms.utils import certification as crt

from kubernetes import client, watch, config
from kubernetes.client.rest import ApiException
from kubernetes.config.config_exception import ConfigException


try:
    config.load_incluster_config()
except ConfigException:
    config.load_kube_config()

CONFIG = client.Configuration()


def get_root_certificate():
    with open(CONFIG.ssl_ca_cert) as f:
        return f.read()


class KubeCertificateSigning(crt.CertificateSigning):
    def __init__(self):
        self._metadata = client.V1ObjectMeta()

    def sign_certificate(self, request):
        """

        Parameters
        ----------
        request : crt.CertificateSigningRequest

        Returns
        -------

        """
        response = self._send_certificate_request(self._get_certificate_signing_request(request, request.certificate))
        if response is None:
            raise RuntimeError('Certificate signing request denied')
        return response

    def _send_certificate_request(self, body):
        try:
            api_instance = client.CertificatesV1beta1Api()
            api_instance.create_certificate_signing_request(body)
            w = watch.Watch()
            # wait for an approval event... if the request gets denied raises an error
            # if the request gets approved fetch the certificate and store it somewhere.
            print('Sending certificate signing request and waiting for a response...')
            for event in w.stream(api_instance.list_certificate_signing_request):
                obj = event['object']
                if event['type'] == 'MODIFIED':
                    condition = obj.status.conditions[-1]
                    if condition.type == 'Denied':
                        return None
                    else:
                        certificate = obj.status.certificate
                        if certificate is not None:
                            print('Certificate signing request was approved!')
                            w.stop()
                            return certificate  # return certificate

        except ApiException as e:
            # catches conflict exceptions as-well.
            print(
                "Exception when calling CertificatesV1beta1Api->create_certificate_signing_request: {}\n".format(e))
            return None

    def _get_certificate_signing_request(self, request, csr):
        metadata = client.V1ObjectMeta()
        metadata.name = request.name
        metadata.namespace = request.namespace
        metadata.groups = request.groups
        metadata.usages = request.usages
        body = client.V1beta1CertificateSigningRequest()
        body.api_version = 'certificates.k8s.io/v1beta1'
        body.kind = 'CertificateSigningRequest'
        body.metadata = metadata
        t = b64encode(csr)
        body.spec = client.V1beta1CertificateSigningRequestSpec(
            request=t.decode(),
            groups=request.groups,
            usages=request.usages
        )
        return body