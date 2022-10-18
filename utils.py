def read_views(file):
    view_dict = {}

    with open(file) as f:
        for view in f.read().split('\n\n'):
            key, *val = view.split('\n')
            view_dict[key] = ' '.join(val)
    return view_dict