import psycopg2
import numpy as np
from scipy.stats import entropy, wasserstein_distance



def distance(target, reference, measure='kld'):

    target = dict(target)
    reference = dict(reference)

    t = list()
    r = list()
    for key in set().union(target.keys(), reference.keys()):
        t_val = target.get(key) if target.get(key) > 0 else np.finfo(float).eps
        r_val = reference.get(key) if reference.get(key) > 0 else np.finfo(float).eps
        t.append(t_val)
        r.append(r_val)

    if measure == 'kld':
        return kld(t, r)

    elif measure == 'emd':
        return earth_movers_distance(t, r)

    else:
        print "Warning! You must choose a distance measure (kl-divergence or earth movers distance)"

def kld(target, reference):
    #tgt_val = [float(x[1]) if x[1] > 0 else np.finfo(float).eps for x in target]
    #ref_val = [float(x[1]) if x[1] > 0 else np.finfo(float).eps for x in reference]
    return entropy(target,reference)

def earth_movers_distance(target, reference):
    t = [float(x[1]) for x in target]
    r = [float(x[1]) for x in reference]
    return wasserstein_distance(t, r)

