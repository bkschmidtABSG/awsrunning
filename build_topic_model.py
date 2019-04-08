#!/usr/bin/env python3
"""
Build a topic model from abstracts (or other text documents) using
anchor_topic.

Command line arguments:
     -i <fname>  Glob for abstracts (use quotes if this has wild cards)
     -o <fname>  Filename for output (defaults to stdout)
     -x          Output to Excel format (incompatible with output to stdout)
     -s <fname>  Filename for stopwords (optional)
     -n <int>    Max number of abstracts to read, default 12,000
     -l <int>    Minimum length of words in characters (default 2)
     -a <int>    Number of anchors (topics), default 50
     -w <int>    Number of words in each topic, default 20

Assumes abstracts are contained in one or more text files. Each line of each
text file corresponds to a unique abstract.  A line consists of two or more
space-separated tokens.  The first token is interpreted as the abstract's
ID, while remaining tokens constitute the abstract.  Tokens should be normalized,
e.g. they should probably be lower-cased, punctuation should be omitted, etc.
The following are removed by this program (although they may also be removed
during pre-processing):
  1) Stop-words (as listed in the stopwords file)
  2) Numbers
  3) Short words (those shorter than the size specified by the -l arg)

Imports anchor_topic, which allows interactive topic modeling.  Documentation here:
    https://github.com/forest-snow/anchor-topic
For the interactive part, see:
    https://github.com/forest-snow/anchor-topic#updating-topics

Output format:
If the -x arg is provided, output is in Excel format, with each row constituting
a record representing a topic.  The Anchor is in column 1, and the list of words
for the topic is in column 2, comma-delimited.  Otherwise (if there is no -x arg),
each record is written on two lines, with the anchor on one line, and the related
words written in comma-delimited form to the next line.  Each record in this text
format is followed by a blank line.

ToDo:
1) For purposes of preventing memory overflow, should we be counting number of
   abstracts, or words in those abstracts (or both?)
2) Would a newer version of scipy help with memory issues?  We have v0.18.
   Latest version of scipy is 1.2.1, see:
       https://docs.scipy.org/doc/scipy-1.2.1/reference/
       https://github.com/scipy/scipy/releases
   Version 0.19 adds a way to dump sparse matrices to files, which could speed up
   debugging, by adding the ff. two command-line args (see commented-out call to
   scipy.sparse.save_npz() below):
      -m <fname>  Use <fname> for matrixWordDoc; depending on -w flag, may mean
                  compute the matrix from abstracts and store it to <fname>, or
                  read the matrix from <fname>.  If there is no -m arg, the matrix
                  will be computed from the abstracts, but it will not be written
                  to a file.
      -w          If present, compute the matrix from abstracts and store it to
                  the file named by the -m arg; else, if there is a -m arg, then
                  read the matrix from that file.  Error if this arg is provided
                  and there is no -m arg.
3) Add filtering to selection of abstracts, using regex's suggested by Steve Sin.

# Authors: Aric Bills, Mike Maxwell: ARLIS, University of Maryland
"""

import argparse    #Command line switch handling
import codecs
import xlsxwriter  #Output to Excel format
from glob import glob
from collections import Counter
import logging
from pathlib import Path
from scipy import sparse
import re
import sys
import time
from anchor_topic.topics import model_topics


def GetCmdLineParameters():
    """Return a tuple of args based on command line parameters.
    """
    parser = argparse.ArgumentParser(description="Build a topic model")
    parser.add_argument( "-i", "--InputGlob"
                       , dest    = "sInputGlob"
                       , metavar = "<InputGlob>"
                       , type    = str
                       , default = 'gsresults.txt'
                       #, default = '/groups/identdata/topictracking/pubmed/abstracts_0.1pct.txt'
                       , help    = "Glob of files to read (quote if contains wildcards)"
                       )
    parser.add_argument( "-o", "--output"
                       , dest    = "sOutFileName"
                       , metavar = "<OutFileName>"
                       , default = "stdout"
                       , help    = "Takes arg <OutputFile>. Optional, defaults to stdout."
                       )
    parser.add_argument( "-x" "-excel"
                       , dest    = "bExcel"
                       , action  = "store_true"
                       , default = False
                       , help    = "Optional; if used, output in Excel format"
                       )
    parser.add_argument( "-s", "--StopWordsFile"
                       , dest    = "sStopWordsFName"
                       , metavar = "<StopWordsFileName>"
                       , type    = str
                       , default = 'stopwords.txt'
                       , help    = "Filename of stop words"
                       )
    parser.add_argument( "-n", "--MaxAbstracts"
                       , type    = int
                       , dest    = "iMaxAbstracts"
                       , metavar = "<MaxAbstracts>"
                       , help    = "Maximum number of abstracts to read"
                       , default = 12000
    #With 64G memory, this works with 6500, crashes with MemoryError at 7500;
    # see notes in build_matrix().  With 128G memory, succeeds with 12,000,
    # (= 74,000 words) and dies with 13,000 (= 78,500 words).
    #Fix: Would tracking word count be more accurate than abstract count?
    #Fix: e.g. abstracts vary in word length.  In the test data, 6500 abstracts
    #Fix: = 52,145 words.
                       )
    parser.add_argument( "-l", "--MinWordLength"
                       , type    = int
                       , dest    = "iMinWordLength"
                       , metavar = "<MinWordLength>"
                       , default = 2
                       , help    = "Minimum length of tokens, in characters"
                       )
    parser.add_argument( "-a", "--NumAnchors"
                       , type    = int
                       , dest    = "iNumAnchors"
                       , metavar = "<NumberOfAnchors>"
                       , default = 50
                       , help    = "Number of anchors to create"
                       )
    parser.add_argument( "-w", "--NumWords"
                       , type    = int
                       , dest    = "iNumWords"
                       , metavar = "<NumberOfWords>"
                       , default = 20
                       , help    = "Number of words to output for each topic"
                       )

    args = parser.parse_args()
    #Open output (we don't open the input, because it's a glob; rather, we open
    # each input file separately, below):
    if args.sOutFileName == 'stdout' and args.bExcel:
        sys.stderr.write("Excel output incompatible with stdout.\n")
        exit(1)
    #If we get here, either output is text to stdout, or we're outputting to a file.
    if args.sOutFileName == 'stdout':
        strOut = codecs.getwriter('utf-8')(sys.stdout.buffer)
    else:
        if args.bExcel:
            strOut = xlsxwriter.Workbook(args.sOutFileName)
        else:
            strOut = open(args.sOutFileName, 'w+', encoding='utf-8')

    return (args.sInputGlob, strOut, args.bExcel, args.sStopWordsFName, \
            args.iMaxAbstracts, args.iMinWordLength, args.iNumAnchors, \
            args.iNumWords)



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
    try:
        with Path(sStopWordsFile).open('r', encoding='utf-8') as strStopWordsFile:
            return set(strStopWordsFile.read().splitlines())
    except (FileNotFoundError, PermissionError, IOError):
        sys.stderr.write("Unable to open stop words file '%s'\n"
            %sStopWordsFile)
        exit(1)


def get_words_and_documents(PathList, iMaxAbstracts, iMinWordLength, StopWords=set()):
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
        iMaxAbstracts: Maximum number of abstracts to read
        iMinWordLength: Minimum word length, in characters
        StopWords (set of str): a set of words to ignore.

    Returns:
        A tuple of (<list of str>, <list of str>): a list of all words in
        the corpus, and a list of all document IDs in the corpus.

    Side-effects:
        writes to log.
    """
    rxNum = re.compile("[\+\-]?[0-9\.\,]\%?") #Abstracts contain lots of numbers,
       # don't want to capture those
    Words = set()
    Docs  = list()
    gwd_logger = logging.getLogger('get_words_...')
    tl = time_logger(gwd_logger)
    iAbstractsPreviousFiles = 0
    for iFile, sFileName in enumerate(PathList, start=1):
        if iAbstractsPreviousFiles > iMaxAbstracts:
            sys.stderr.write("Too many abstracts, skipping file %s\n"
                %sFileName)
            break
        else:
            sys.stderr.write("Reading file %i = '%s'\n" %(iFile, sFileName))
        try:
            with Path(sFileName).open('r', encoding='utf-8') as strFile:
                for iAbstract, sAbstract in enumerate(strFile, start=iAbstractsPreviousFiles):
                    #Init count with the count of abstracts found in previous files
                    if iAbstract > iMaxAbstracts:
                        break #Both from this loop, and from the outer for-loop
                    next(tl)
                    tl.send(('info', 'file {}, sAbstract {}'.format(sFileName, iFile)))
                    Values = sAbstract.strip().split(' ')
                    Docs.append(Values[0])
                    NewWords = (set(Values[1:]) - StopWords)
                    for sToken in sorted(NewWords):
                        if '--' in sToken or \
                           sToken[0] == '-' or \
                           rxNum.match(sToken) or \
                           len(sToken) < iMinWordLength:
                            NewWords.remove(sToken)
                    Words |= NewWords
                iAbstractsPreviousFiles = iAbstract #For next file
        except (FileNotFoundError, PermissionError, IOError):
            sys.stderr.write("Unable to open abstracts file '%s'\n" %sFileName)
            exit(1)
    return sorted(Words), Docs


def build_matrix(PathList, WordsInCorpus, Docs, iMaxAbstracts):
    """
    Create a sparse matrix containing the counts of each word in each
    document in the corpus.

    Args:
        PathList (list of pathlib.Path): paths to input files.  Each input file
           contains one abstract per line; the string before the first space
           in each line is the PubMed ID for the abstract.  The other tokens
           on the line are preprocessed tokens from the abstract, separated by
           spaces.
        WordsInCorpus (list of str): the complete list of words that appear in
            the corpus.
        Docs (list of str): the complete list of document IDs that
            occur in the corpus.
        iMaxAbstracts

    Returns:
        (scipy.sparse.csc_matrix): a matrix where each row represents a
            word, each column represents a document, and each cell
            represents the frequency of a given word in a given
            document.
    """
    DocIndex = {sDocID: iDoc for iDoc, sDocID in enumerate(Docs)}
    WordIndex = {sWord: iWord for iWord, sWord in enumerate(WordsInCorpus)}
    buildm_logger = logging.getLogger('build_matrix')
    tl = time_logger(buildm_logger)
    matrixWordDoc = sparse.lil_matrix((len(WordsInCorpus), len(Docs)), dtype=int)
    #Attempted this with various types of sparse matrices; see documentation of
    # these at https://rushter.com/blog/scipy-sparse-matrices/.  Results:
    #   bsr_matrix: Fails with NotImplementedError
    #   coo_matrix: Does not support item assignment
    #   csc_matrix: Slow, succeeds with 5000 abstracts, dies (in search.row_normalize
    #     at copy() with 10,000 abstracts.  Re the speed, it outputs a warning:
    #     "SparseEfficiencyWarning: Changing the sparsity structure of a csc_matrix is expensive."
    #   dia_matrix: Does not support item assignment
    #   dok_matrix: Fast, succeeds with 5000 (2m 9s real time.  With 64G memory,
    #      dies with 7500 abstracts.
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
    #   lil_matrix: Fast, succeeds with 5000 (2m 3s real time).  With 64G memory,
    #      dies with 7500 abstracts.
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
    iAbstractsPreviousFiles = 0
    for iFile, sFileName in enumerate(PathList, start=1):
        with Path(sFileName).open('r', encoding='utf-8') as strFile:
            for iAbstract, sAbstract in enumerate(strFile, start=iAbstractsPreviousFiles):
                #Init count with the count of abstracts found in previous files
                if iAbstract > iMaxAbstracts:
                    break
                next(tl)
                tl.send(('info', 'file {}, sAbstract {}'.format(iFile, iAbstract)))
                #Tokenize:
                TokensInAbstract = [sWordToken for sWordToken in sAbstract.strip().split(' ')]
                #tokens --> types with count:
                WordCounts = Counter([sWordToken for sWordToken in TokensInAbstract[1:] \
                                        if sWordToken in WordIndex])
                    #Starting TokensInAbstract at [1] means we skip the first "token"
                    # in sAbstract, which is actually the document ID
                for sWordType in WordCounts: #sWord is a key in WordCounts
                    matrixWordDoc[WordIndex[sWordType], DocIndex[TokensInAbstract[0]]] = \
                        WordCounts[sWordType]
            iAbstractsPreviousFiles = iAbstract #For next file
    return matrixWordDoc.tocsc()


# =============== MAIN ===================
(sInputGlob, strOut, bExcel, sStopWordsFName, iMaxAbstracts, iMinWordLength, iNumAnchors, iNumWords) \
    = GetCmdLineParameters()
StopWords = read_stopwords(sStopWordsFName)
PathList = glob(sInputGlob)
Words, Docs = get_words_and_documents(PathList, iMaxAbstracts, iMinWordLength, StopWords)
sys.stderr.write("Read %i abstracts, containing %i Words.\n"
    %(len(Docs), len(Words)))
matrixWordDoc = build_matrix(PathList, Words, Docs, iMaxAbstracts)
#Fix: why do we pass PathList and iMaxAbstracts to both get_words_and_documents()
#Fix: and build_matrix()?

#Fix: Ff requires scipy_sparse v0.19, we have 0.18
#Fix: scipy.sparse.save_npz("matrixWordDoc.npz", matrixWordDoc)
    #Debug: Save the above matrix so we don't have to rebuild it while
    #Debug: with changes to other modules.
matrixWordTopic, matrixWordCoocur, Anchors = \
   model_topics(M=matrixWordDoc, k=iNumAnchors, threshold=0.01)
  #Documentation for model_topics() at
  #    https://github.com/forest-snow/anchor-topic
  # Args:
  #   M         = a word-document matrix
  #   k         = number of topics
  #   threshold = minimum percentage of document occurrences for word to be
  #               considered as an anchor candidate  (How to set this?)
  #Outputs:
  # A       = word-topic matrix
  # Q       = word-cooccurrence matrix
  # Anchors = 2D list of anchor words for each topic
if bExcel:
    TextFormat = strOut.add_format()
    TextFormat.set_align('vjustify')   #'vjustify' means wrapped
    strWorksheet = strOut.add_worksheet()
    strWorksheet.set_default_row(30)  #Sets height; default is 15 (units of what?)
    strWorksheet.set_column(0, 0,  25, TextFormat) #Column A:  25 "default" characters wide
    strWorksheet.set_column(1, 1, 125, TextFormat) #Column B: 125 "default" characters wide
for iAnchor, Anchor in enumerate(Anchors, start=0):
    if bExcel:
        strWorksheet.write("A%i" %(iAnchor+1), " ".join(Words[iAnchor] for iAnchor in Anchor))
    else: #Text output
        strOut.write("%s\n" %" ".join(Words[iAnchor] for iAnchor in Anchor))
    TopicWords = []
    for iWord in list(matrixWordTopic[:,iAnchor].argsort())[:-(iNumWords+1):-1]:
        TopicWords.append(Words[iWord])
    if bExcel:
        strWorksheet.write("B%i" %(iAnchor+1), ", ".join(TopicWords))
    else: #Text output
        strOut.write("%s\n\n" %", ".join(TopicWords))
strOut.close()
