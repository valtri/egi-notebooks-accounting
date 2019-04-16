NAMESPACE_FILE = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'


def get_k8s_namespace():
    with open(NAMESPACE_FILE, 'r') as f:
        return f.read()
