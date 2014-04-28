import pkg_resources


def select_source(type_):
    entry_points = list(
        pkg_resources.iter_entry_points('backy.sources', type_))
    return entry_points[0].load()
