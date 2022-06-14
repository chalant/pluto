from collections import defaultdict
idx = 0

components = defaultdict(list)


class Entity(object):
    def __init__(self):
        pass


class Component(object):
    def __init__(self):
        pass

    def update(self, data):
        pass


def add_component(component_type, component):
    components[component_type].append(component)


def get_components(component_type):
    return components[component_type]
