#!/usr/bin/env python3
"""
Build a topic model from PubMed data using anchor_topic.topics.

Assumes the PubMed data is in one or more text files. Each line
corresponds to a unique abstract in the corpus. The string before the
first space in the line is the PubMed ID for the abstract. The other
items on the line are preprocessed tokens from the abstract. Tokens
are separated by spaces.
"""

from collections import Counter
import logging
from pathlib import Path
import scipy
import sys
import time

from anchor_topic.topics import model_topics

iMAX_N = 6500  #Max number of abstracts to try
    #Works with 6500, crashes with MemoryError at 7500; see notes in
    # build_matrix()


#Set up logging:
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(name)-12s %(levelname)-8s %(message)s",
    datefmt="%m-%d %H:%M",
    filename="build_topic_model.log"
)
console = logging.StreamHandler()
console.setLevel(logging.INFO) #Send all logging msgs to console
formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
console.setFormatter(formatter)
logging.getLogger('').addHandler(console)


def time_logger(logger, interval=5):
    """
    Create a coroutine that logs at intervals.

    Args:
        interval (float): number of seconds to wait between logging.

    Returns a generator.

    Accepts a level (debug, info, warning, or error) and a message using
    the generator's send() method.
    """
    t = time.time()
    while True:
        lvl, msg = yield
        yield
        if time.time() > t + interval:
            fn = getattr(logger, lvl)
            fn(msg)
            t = time.time()


def read_stopwords(sStopWordsFile):
    """Read StopWords from a file and return them as a set."""
    with Path(sStopWordsFile).open('r', encoding='utf-8') as strStopWordsFile:
        return set(strStopWordsFile.read().splitlines())


def get_words_and_documents(PathList, StopWords=set()):
    """
    Identify all words and document IDs in the corpus.

    Args:
        PathList (list of pathlib.Path): paths to input files.  Each input file
           contains one abstract per line; the string before the first space
           in each line is the PubMed ID for the abstract.  The other tokens
           on the line are preprocessed tokens from the abstract, separated by
           spaces.
           #Fix: Perhaps better would be tab-delimited with two fields: ID and token
           #Fix: list, the latter space-delimited.  That would avoid the references
           #Fix: to [1:] below.  But that requires a change to the abstract files.
        StopWords (set of str): if not None, a set of words to ignore.

    Returns:
        (tuple of (list of str), (list of str)): a lsit of all words in
        the corpus and a list of all document IDs in the corpus.

    Side-effects:
        writes to log.
    """
    Words = set()
    Docs  = list()
    gwd_logger = logging.getLogger('get_words_...')
    tl = time_logger(gwd_logger)
    iAbstractsPreviousFiles = 0
    for iFile, sFileName in enumerate(PathList, start=1):
        with sFileName.open('r', encoding='utf-8') as strFile:
            for iAbstract, sAbstract in enumerate(strFile, start=iAbstractsPreviousFiles):
                #Init count with the count of abstracts found in previous files!
                if iAbstract > iMAX_N:
                    break
                next(tl)
                tl.send(('info', 'file {}, sAbstract {}'.format(sFileName, iFile)))
                Values = sAbstract.strip().split(' ')
                Docs.append(Values[0])
                NewWords = (set(Values[1:]) - StopWords)
                for item in sorted(NewWords):
                    if '--' in item or item[0] == '-':
                        NewWords.remove(item)
                Words |= NewWords
            iAbstractsPreviousFiles = iAbstract #For next file
    return sorted(Words), Docs


def build_matrix(PathList, Words, Docs, StopWords=set()):
    """
    Create a sparse matrix containing the counts of each word in each
    document in the corpus.

    Args:
        PathList (list of pathlib.Path): paths to input files.  Each input file
           contains one abstract per line; the string before the first space
           in each line is the PubMed ID for the abstract.  The other tokens
           on the line are preprocessed tokens from the abstract, separated by
           spaces.
        Words (list of str): the complete list of words that appear in
            the corpus.
        Docs (list of str): the complete list of document IDs that
            occur in the corpus.
        StopWords (set of str): if not None, a set of words to ignore.

    Returns:
        (scipy.sparse.csc_matrix): a matrix where each row represents a
            word, each column represents a document, and each cell
            represents the frequency of a given word in a given
            document.
    """

    DocIndex = {sDocID: iDoc for iDoc, sDocID in enumerate(Docs)}
    WordIndex = {sWord: iWord for iWord, sWord in enumerate(Words)}
    buildm_logger = logging.getLogger('build_matrix')
    tl = time_logger(buildm_logger)
    #Attempted this with various types of sparse matrices; see documentation of
    # these at https://rushter.com/blog/scipy-sparse-matrices/.  Results:
    #   bsr_matrix: Fails with NotImplementedError
    #   coo_matrix: Does not support item assignment
    #   csc_matrix: Slow, succeeds with 5000 abstracts, dies (in search.row_normalize
    #     at copy() with 10,000 abstracts.  Re the speed, it outputs a warning:
    #     "SparseEfficiencyWarning: Changing the sparsity structure of a csc_matrix is expensive."
    #   dia_matrix: Does not support item assignment
    #   dok_matrix: Fast, succeeds with 5000 (2m 9s real time), dies with 7500.
    #      Error msg:
    #       Traceback (most recent call last):
    #           File "build_topic_model.py", line 153, in <module> #Line # may have changed!
    #              A, Q, Anchors = model_topics(Matrix, 10, 0.01)
    #           File "/usr/lib/python3.4/site-packages/anchor_topic/topics.py",
    #              line 81, in model_topics
    #              anchors = search.greedy_anchors(Q, k, candidates, seed)
    #           File "/usr/lib/python3.4/site-packages/anchor_topic/search.py",
    #              line 21, in greedy_anchors
    #              Q_bar = row_normalize(Q)
    #           File "/usr/lib/python3.4/site-packages/anchor_topic/search.py",
    #              line 9, in row_normalize
    #              Q_new = Q.copy()
    #       MemoryError
    #   lil_matrix: Fast, succeeds with 5000 (2m 3s real time), dies with 7500.
    #      Error msg:
    #       Traceback (most recent call last):
    #           File "build_topic_model.py", line 188, in <module> #Line # may have changed!
    #              model_topics(M=matrixWordDoc, k=20, threshold=0.01)
    #           File "/usr/lib/python3.4/site-packages/anchor_topic/topics.py",
    #              line 84, in model_topics  #Later than with dok_matrix!
    #              A = recover.computeA(Q, anchors)
    #           File "/usr/lib/python3.4/site-packages/anchor_topic/recover.py",
    #              line 101, in computeA
    #              P_w = numpy.diag(Q.sum(axis=1))
    #           File "/usr/lib64/python3.4/site-packages/numpy/lib/twodim_base.py",
    #              line 255, in diag
    #              res = zeros((n, n), v.dtype)
    #       MemoryError
    matrixWordDoc = scipy.sparse.lil_matrix((len(Words), len(Docs)), dtype=int)
    iAbstractsPreviousFiles = 0
    for iFile, sFileName in enumerate(PathList, start=1):
        with sFileName.open('r', encoding='utf-8') as strFile:
            for iAbstract, sAbstract in enumerate(strFile, start=iAbstractsPreviousFiles):
                #Init count with the count of abstracts found in previous files
                if iAbstract > iMAX_N:
                    break
                next(tl)
                tl.send(('info', 'file {}, sAbstract {}'.format(iFile, iAbstract)))
                #Tokenize:
                WordTokens = sAbstract.strip().split(' ')
                #tokens --> types with count:
                WordCounts = Counter([sWordToken for sWordToken in WordTokens[1:] \
                                        if sWordToken in WordIndex])
                for sWordType in WordCounts: #sWord is a key in WordCounts
                    matrixWordDoc[WordIndex[sWordType], DocIndex[WordTokens[0]]] \
                        = WordCounts[sWordType]
            iAbstractsPreviousFiles = iAbstract #For next file
    return matrixWordDoc.tocsc()


# temporary
StopWords = read_stopwords('stopwords.txt')
PathList = [Path('gsresults.txt')]
#Fix: make above path a command line arg
Words, Docs = get_words_and_documents(PathList, StopWords)
sys.stdout.write("Read %i abstracts, containing %i Words.\n"
    %(len(Docs), len(Words)))
matrixWordDoc = build_matrix(PathList, Words, Docs, StopWords)
matrixWordTopic, matrixWordCoocur, Anchors = \
   model_topics(M=matrixWordDoc, k=50, threshold=0.01)
  #Documentation for model_topics() at
  #    https://github.com/forest-snow/anchor-topic
  # Args:
  #   M         = a word-document matrix
  #   k         = number of topics
  #   threshold = minimum percentage of document occurrences for word to be
  #               considered as an anchor candidate
  #Outputs:
  # A       = word-topic matrix
  # Q       = word-cooccurrence matrix
  # Anchors = 2D list of anchor words for each topic
sys.stdout.write("Anchor words:\n")
for Anchor in Anchors:
    sys.stdout.write("%s\n" %" ".join(Words[iAnchor] for iAnchor in Anchor))