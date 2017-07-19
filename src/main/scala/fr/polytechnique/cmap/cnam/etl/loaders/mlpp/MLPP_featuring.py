"""Convert csv files resulting from MLPP featuring to CSR sparse matrices."""
import glob
import pickle
from os import walk
from os.path import join

import numpy as np
import pandas as pd
from scipy.sparse import csr_matrix


def sparse_features(root, metadata):
    """Dump the features sparse matrix."""
    data = pd.read_csv(glob.glob(join(root, 'SparseFeatures.csv/part-00000*'))[0])

    shape = (metadata['buckets'], metadata['columns'])

    arr = [csr_matrix(shape)] * metadata['patients']

    p_indices = data['patientIndex'].unique()

    for p_idx in p_indices:
        df = data[data['patientIndex'] == p_idx]
        arr[p_idx] = csr_matrix((df['value'], (df['bucketIndex'], df['colIndex'])), shape=shape)

    with open(join(root, 'sparse_features.pickle'), 'wb') as f:
        pickle.dump(arr, f, -1)


def outcomes(root, metadata):
    """Dump the outcomes sparse matrix."""
    data = pd.read_csv(glob.glob(join(root, 'Outcomes.csv/part-00000*'))[0])

    shape = (metadata['buckets'], 1)

    arr = [csr_matrix(shape)] * metadata['patients']

    p_indices = data['patientIndex'].unique()

    for p_idx in p_indices:
        df = data[data['patientIndex'] == p_idx]
        ones = np.full(len(df.index), 1)
        arr[p_idx] = csr_matrix((ones, (df['bucket'], ones-1)), shape=shape)

    with open(join(root, 'outcomes.pickle'), 'wb') as f:
        pickle.dump(arr, f, -1)


def static_outcomes(root, metadata):
    """Dump the static outcomes sparse matrix."""
    static = pd.read_csv(glob.glob(join(root, 'StaticOutcomes.csv/part-00000*')
                                  [0])).values
    ones = np.full(sparse_outcomes.size, 1)

    shape = (metadata['patients'], 1)

    result = csr_matrix((ones, (sparse_outcomes[:, 0], ones-1)),
                        shape=shape).todense()

    with open(join(root, 'static_outcomes.pickle'), 'wb') as f:
        pickle.dump(result, f, -1)

def censoring(root, metadata):
    """Dump the outcomes sparse matrix."""
    data = pd.read_csv(glob.glob(join(root, 'Censoring.csv/part-00000*'))[0])

    arr = [metadata['buckets']] * metadata['patients']

    p_indices = data['patientIndex'].unique()

    for p_idx in p_indices:
        df = data[data['patientIndex'] == p_idx]
        arr[p_idx] = df['bucket'].iloc[0]

    with open(join(root, 'censoring.pickle'), 'wb') as f:
        pickle.dump(arr, f, -1)


if __name__ == '__main__':
    for root, dirs, files in walk("./"):
        if len(root) > 4 and root[-4:] == "/csv":
            print("Processing " + root)
            metadata = pd.read_csv(glob.glob(join(root, 'metadata.csv/part-00000*'))[0]
                                   ).to_dict('records')[0]
            sparse_features(root, metadata)
            outcomes(root, metadata)
            censoring(root, metadata)
            # static_outcomes(root, metadata)
