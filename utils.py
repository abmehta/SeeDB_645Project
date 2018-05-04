import psycopg2
import numpy as np
from scipy.stats import entropy, wasserstein_distance

eps = np.finfo(float).eps

def distance(target, reference, measure='kld'):

    target = dict(target)
    reference = dict(reference)

    t = list()
    r = list()

    # we need this because this ensures that there are the same number of categories in each distribution
    for key in set().union(target.keys(), reference.keys()):
        t_val = float(target.get(key, 0))
        r_val = float(reference.get(key, 0))
        t.append(t_val)
        r.append(r_val)


    # normalize to create a probability distribution
    t = np.asarray(t) / (np.sum(t) or 1.0)
    r = np.asarray(r) / (np.sum(r) or 1.0)
    t = [max(x, eps) for x in t]
    r = [max(x, eps) for x in r]
    if measure == 'kld':
        return kl_divergence(t, r)

    elif measure == 'emd':
        return earth_movers_distance(t, r)

    else:
        print "Warning! You must choose a distance measure (kl-divergence or earth movers distance)"

def kl_divergence(target, reference):
    return entropy(target,reference)

def earth_movers_distance(target, reference):
    return wasserstein_distance(target, reference)

