#/bin/env python3
"""
Import a set of abstracts into the format needed for topic modeling, retaining
author/title info in a file.

Arguments:
   -i <Input File Name> (optional, defaults to stdin.  See below re format.)
   -e <Input encoding>  (optional, defaults to UTF-8.  Output is always UTF-8.)
   -x <Index File Name) (Obligatory.  See below for format.  If it already exists,
      new entries will be appended at the end.)
   -o <Output File Name) (for abstracts; optional, defaults to stdout.  See
      below for format.)

Input format:
Each abstract consists of three lines: Title, Authors, and text.  Abstracts are
separated by several newlines.

Output format for index:
Tab-separated records: ID\tAuthors\tTitle

Output format for abstracts:
ID, space character, space-separated tokens of abstract

WARNING: This does not check for duplicate abstracts!  In particular, running
this program twice on the same input file will result in all that file's abstracts
being duplicated.
"""

from argparse import ArgumentParser
import sys, codecs
from pathlib import Path


def GetCmdLineParameters():
    """Return a tuple of args based on command line parameters.
    """

    parser = ArgumentParser(description="Import abstracts in title-author-text format, export for topic modeling")
    parser.add_argument( "-i", "--InputFName"
                       , dest    = "sInFName"
                       , metavar = "<InFileName>"
                       , type    = str
                       , default = 'stdin'
                       , help    = "Takes arg <OutputFile>. Optional, defaults to stdin."
                       )
    parser.add_argument( "-x", "--IndexFName"
                       , dest    = "sIndexFName"
                       , metavar = "<IndexFileName>"
                       , help    = "Takes arg <IndexFile>. Obligatory."
                       )
    parser.add_argument( "-o", "--OutFName"
                       , dest    = "sOutFName"
                       , metavar = "<OutFileName>"
                       , default = "stdout"
                       , help    = "Takes arg <OutputFile>. Optional, defaults to stdout."
                       )
    parser.add_argument( "-e", "--encoding"
                       , dest    = "sEncoding"
                       , metavar = "<encoding>"
                       , default = "utf-8"
                       , help    = "Encoding for input file, e.g. 'cp1252'.  Optional, defaults to UTF-8."
                       )
    args = parser.parse_args()

    #Get encoding:
    sEncoding = args.sEncoding.lower()
    KnownEncodings = ['cp1252', 'utf-8', 'utf-16', 'latin-1', 'iso-8859-1']
    if sEncoding not in KnownEncodings:
        sys.stderr.write("Unknown encoding %s.  Known encodings are %s.\n"
            %(args.sEncoding, ', '.join(KnownEncodings)))
        exit(1)

    #Open input:
    try:
        if args.sInFName == 'stdout':
            strIn = codecs.getwriter(sEncoding)(sys.stdin.buffer)
        else:
            strIn = open(args.sInFName, 'r', encoding=sEncoding)
    except:
        sys.stderr.write("Failed to open input file %s for reading"
            %args.sInFName)
        exit(1)
    #Open output:
    try:
        if args.sOutFName == 'stdout':
            strOut = codecs.getwriter('utf-8')(sys.stdout.buffer)
        else:
            strOut = open(args.sOutFName, 'w+', encoding='utf-8')
    except:
        sys.stderr.write("Failed to open output file %s for writing"
            %args.sOutFName)
        exit(1)
    #Open index file.  If the file already exists, read the last line and get
    # the last index number.  Else create a new starting index number.
    if Path(args.sIndexFName).exists():
        try:
            strIndex = open(args.sIndexFName, 'r', encoding='utf-8')
        except:
            sys.stderr.write("Failed to open index file %s for reading"
                %args.sIndexFName)
        #If we get here, we were able to open the existing index file:
        sLine = '0' #Dummy, in case index exists but is empty
        for sLine in strIndex:
            pass #Inefficient
        iID = int(sLine.split('\t')[0]) + 1
        strIndex.close()
    else:
        iID = 1
    strIndex = open(args.sIndexFName, 'a', encoding='utf-8')

    return (strIn, strOut, strIndex, iID)


def ProcessNextAbstract(strIn, strOut, strIndex, iID):
    """Read the next abstract from strIn, write the index information to strIndex,
       and the ID + text to strOut.  (See top of file for format of these three
       files.)  Return the ID of this abstract, or None at EOF.
    """
    sTitle = strIn.readline()
    if not sTitle: #EOF
        return None
    #If we get here, we must have read a non-empty line
    sTitle = sTitle.strip()
    while not sTitle: #Skip any empty lines
        sTitle = strIn.readline()
        if not sTitle:
            return None #Must have skipped some blank lines, and now we're at EOF
        sTitle = sTitle.strip()
    sAuthors = strIn.readline().strip()
    sText = strIn.readline().strip()
    if (not sTitle) or (not sAuthors) or (not sText):
        sys.stderr.write("Found incomplete abstract; authors = '%s', title = '%s', text = '%s'.  Skipping."
            %(sAuthors, sTitle, sText))
    else:
        strIndex.write("%i\t%s\t%s\n" %(iID, sAuthors, sTitle))
        strOut.write("%i %s\n" %(iID, " ".join(sText.split())))
        iID += 1
    return iID



if __name__ == '__main__':
    (strIn, strOut, strIndex, iID) = GetCmdLineParameters()

    iNewID = ProcessNextAbstract(strIn, strOut, strIndex, iID)
    while iNewID:
        iNewID = ProcessNextAbstract(strIn, strOut, strIndex, iNewID)
    strIn.close()
    strOut.close()
    strIndex.close()
