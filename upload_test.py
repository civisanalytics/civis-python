from datetime import datetime
import resource
import civis
import logging

log = logging.getLogger(__name__)


def main():
    client = civis.APIClient()
#    file = '/Users/jkulzick/py_client/20gb_file.txt'
#    file = '/Users/jkulzick/py_client/1gb_file.txt'
#    file = '/Users/jkulzick/py_client/20mb_file.txt'
#    file = '/Users/jkulzick/py_client/40mb_file.txt'
    file = '/Users/jkulzick/py_client/seq_test.csv'
#    file = '/Users/jkulzick/py_client/seq_comp.csv.gz'
#    file = '/Users/jkulzick/py_client/seq_comp.zip'

    with open(file, 'rb') as f:
        mem_start = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        print('New start: ', datetime.now())
        file_id = civis.io.file_to_civis(f, 'test')
        print('New end: ', datetime.now())
        mem_end = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        print('New mem: ', mem_end-mem_start)
        print('file id: ', file_id)

    with open(file + '.new', 'wb') as fout:
        civis.io.civis_to_file(file_id, fout)

if __name__ == '__main__':
    main()
