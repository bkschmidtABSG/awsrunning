#/bin/env python3
"""
Use a list of IDs from a PubChem search to create a file containing a subset
of the PubMed abstract corpus, tokenized.  Used to build an input corpus for
build_topic_model.py.

Arguments:
  -x   Excel file containing a list of IDs in column 1
  -r   Root directory for PubMed abstracts, defaults to
          /groups/identdata/topictracking/pubmed/abstracts/
  -m   Max number of abstracts to read  (the only use of the -n argument on
       build_topic_model.py to reduce the number of abstracts to a number
       that that program can handle.
  -o   Destination file, defaults to stdout
  -e   Error output file, defaults to stderr (error output may be voluminous
       if the list of IDs is even a few months newer than the downloaded abstracts)

Expected Excel format:
On the first sheet, the first column will be a list of PubMed IDs.  These will
probably be numbers, but we accept them even if they begin with 'pmid', 'PMID'
etc.  (The length of the integer part of the ID may vary--they don't include
leading zeros.  We compensate...)

Format of PubMed abstracts dir:
The abstracts have been broken into subdirs based on the first 4 digits of the
PubMed ID (including any leading zeros).  Each subdir contains a .txt files
whose file names consist of 'PMID' plus the full PubMed ID.  Each .txt file
contains a single line, the abstract text in UTF-8.
"""

from argparse import ArgumentParser
from pathlib import Path
import codecs
from pandas import read_excel
import regex  #Note regex, not re
import sys



def GetCmdLineParameters():
    """Return a tuple of args based on command line parameters.
    """

    parser = ArgumentParser(description="Use the list of IDs in an Excel file to create a file containing a tokenized subset of the PubMed abstract corpus")
    parser.add_argument( "-x", "--Excel"
                       , dest    = "sExcelFName"
                       , help    = "Excel file to read from."
                       )
    parser.add_argument( "-r", "--PubMedDir"
                       , dest    = "sPubMedDir"
                       , type    = str
                       , default = '/groups/identdata/topictracking/pubmed/abstracts/'
                       , help    = "Root directory for PubMed abstracts, defaults to '/groups/identdata/topictracking/pubmed/abstracts/'"
                       )

    parser.add_argument( "-m", "--MaxAbstracts"
                       , dest    = "iMaxAbstracts"
                       , type    = int
                       , default = 10**7 #Arbitrary
                       , help    = "Max number of abstracts to read (default is very large)."
                       )
    parser.add_argument( "-o", "--output"
                       , dest    = "sOutFileName"
                       , default = "stdout"
                       , help    = "Takes arg <OutputFile>. Optional, defaults to stdout."
                       )
    parser.add_argument( "-e", "--error"
                       , dest    = "sErrFileName"
                       , default = "stderr"
                       , help    = "Takes arg <ErrorFile>. Optional, defaults to stderr."
                       )
    args = parser.parse_args()

    #Open Excel file:
    sys.stderr.write("Reading %s...\n" %args.sExcelFName)
        #read_excel() can be slow, so output status (here and when done)
    try:
        DataFrame = read_excel(args.sExcelFName, usecols=[0])
          #This is a one-time thing; it reads the file, stores the data in DataFrame,
          # and closes the file.  It is also slow, if the Excel file is large.
    except:
        sys.stderr.write("Unable to read file %s as an Excel file\n" %args.sExcelFName)
        exit(1)
    sys.stderr.write("Done reading %s.\n" %args.sExcelFName)
    #Validate PubMed dir:
    if not Path(args.sPubMedDir).is_dir(): #Does not check permissions
        sys.stderr.write("Directory %s does not appear to exist." %args.sPubMedDir)
        exit(1)
    #Open output:
    try:
        if args.sOutFileName == 'stdout':
            strOut = codecs.getwriter('utf-8')(sys.stdout.buffer)
        else:
            strOut = open(args.sOutFileName, 'w+', encoding='utf-8')
    except:
        sys.stderr.write("Failed to open output file %s for writing"
            %args.sOutFileName)
        exit(1)
    #Open error output:
    try:
        if args.sErrFileName == 'stderr':
            strErr = codecs.getwriter('utf-8')(sys.stderr.buffer)
        else:
            strErr = open(args.sErrFileName, 'w+', encoding='utf-8')
    except:
        sys.stderr.write("Failed to open error file %s for writing"
            %args.sErrFileName)
        exit(1)

    return (DataFrame, args.sPubMedDir, strOut, args.iMaxAbstracts, strErr)



def BuildSubset(DataFrame, sPubMedDir, strOut, strErr, iMaxAbstracts):
    """
    Iterate over the list of IDs in DataFrame and extract the relevant abstracts
    from sPubMedDir, outputting them to strOut.
    Args:
        DataFrame:     As returned from Panda's read_excel().  See documentation at:
             https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_excel.html
        sPubMedDir:    Path to the root directory for PubMed abstracts.
        strOut:        Output file stream
        strErr:        Error output stream
        iMaxAbstracts: Max number of abstracts to process; defaults to a very
             large number.
    No return value.
    Side effects:
        Writes one line per sampled abstract to output stream. Each line will
        begin with a PubMed ID followed by a space and one or more space-separated
        word tokens.
    """
    try:
        SubDirs = sorted([path for path in Path(sPubMedDir).glob('*') if path.is_dir()])
    except:
        strErr.write("Failure listing files in root directory %s.  Perhaps you do not have permission to read from this directory?"
                %sPubMedDir)
        exit(1)
    #If we get here, we have permission to read the root directory.  (We'll check
    # each subdir below.)
    for (iRow, PubMedID) in DataFrame.itertuples():
        #We told read_excel() to only return the first column (column 0),
        # so itertuples should yield tuples of len 2
        if iRow >= iMaxAbstracts: #iRow is 0-based
            strErr.write("Stopping at %ith abstract.\n" %iRow)
            return
        sPubMedID = str(PubMedID).lower() #Unclear whether this is a string in the first place
        if sPubMedID.startswith('pmid'):
            sPubMedID = sPubMedID[4:]
        sSubDir   = sPubMedID[0:-4]
        while len(sSubDir) < 4:
            #Subdir names have leading zeros, so pad if needed:
            sSubDir = '0' + sSubDir
        sTxtFName = sPubMedDir + sSubDir + "/PMID" + sPubMedID + ".txt"
        try:
            with open(sTxtFName, 'r', encoding='utf-8') as strTxtFile:
                try:
                    sData = strTxtFile.read().lower().strip()
                except:
                    strErr.write("Failure processing file %s.  Perhaps you do not have read permission on this file?"
                       %sTxtFName)
                    exit(1)
                if not sData:
                    strErr.write("Found empty file %s\n" %sTxtFName)
                    continue
                Tokens = regex.findall(r"[\w-]+", sData, flags=regex.VERSION1)
                strOut.write('{:08d} {}\n'.format(int(sPubMedID), ' '.join(Tokens)))
                #Debug:strOut.write(sPubMedID + " " + sData + '\n')
        except FileNotFoundError:
            strErr.write("Unable to find file for PubMed ID %s; possibly newer than newest downloaded abstract.  Ignoring.\n"
                %sPubMedID)




if __name__ == '__main__':
    (DataFrame, sPubMedDir, strOut, iMaxAbstracts, strErr) = GetCmdLineParameters()
    BuildSubset(DataFrame, sPubMedDir, strOut, strErr, iMaxAbstracts)
    #No need to close the Excel file at the end, that's handled in read_excel()
    strOut.close()
