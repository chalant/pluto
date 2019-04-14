from kubernetes import client, watch, config
from kubernetes.client.rest import ApiException
from kubernetes.config.config_exception import ConfigException

import zipline

try:
    config.load_incluster_config()
except ConfigException:
    config.load_kube_config()

CONFIG = client.Configuration()

print(CONFIG.cert_file)
print(CONFIG.api_key)
# api_instance = client.CertificatesV1beta1Api()
# w = watch.Watch()
# # wait for an approval event... if the request gets denied raises an error
# # if the request gets approved fetch the certificate and store it somewhere.
# for event in w.stream(api_instance.list_certificate_signing_request):
#     print(event)