#/bin/env python3
"""
Create a file containing a subset of the PubMed abstract corpus, tokenized.
Arguments:

"""

from argparse import ArgumentParser
from pathlib import Path
import regex  #Note regex, not re
import sys


iUpdateInterval = 1000 #Output a status message at every N files


def GetCmdLineParameters():
    """Return a tuple of args based on command line parameters.
    """

    parser = ArgumentParser(description="Create a file containing a tokenized subset of the PubMed abstract corpus")
    parser.add_argument( "-r", "--sRootDir"
                       , dest    = "sRootDir"
                       , type    = str
                       , default = '/groups/identdata/topictracking/pubmed/abstracts/'
                       , help    = "Root directory for PubMed abstracts, defaults to '/groups/identdata/topictracking/pubmed/abstracts/'"
                       )
    parser.add_argument( "-o", "--output"
                       , dest    = "sOutFileName"
                       , default = "stdout"
                       , help    = "Takes arg <OutputFile>. Optional, defaults to stdout."
                       )
    parser.add_argument( "-n", "--iNumerator"
                       , dest    = "iNumerator"
                       , default = 1
                       , help    = "Numerator for fraction of files to retain.  Optional, defaults to 1."
                       )
    parser.add_argument( "-d", "--iDenominator"
                       , type    = int
                       , dest    = "iDenominator"
                       , help    = "Denominator for fraction of files to retain.  Optional, defaults to 1000."
                       , default = 1000
                       )
    args = parser.parse_args()

    #Validate input dir:
    if not Path(args.sRootDir).is_dir(): #Does not check permissions
        sys.stderr.write("Directory %s does not appear to exist." %args.sRootDir)
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

    return (args.sRootDir, strOut, args.iNumerator, args.iDenominator)



def build_subset(sRootDir, iNumerator, iDenominator, strOut):
    """
    Iterate over abstracts and extract a subset into a file.

    Args:
        sRootDir:     Path to the root directory for PubMed abstracts.
        iNumerator:   (int) keep this many files out of iDenominator.
        iDenominator: (int) keep iNumerator files out of this many.
        strOut:       Output file stream
    No return value.
    Side effects:
        Writes one line per sampled abstract to output stream. Each line will
        begin with a PubMed ID followed by a space and one or more space-separated
        word tokens.
    """
    Intervals  = [iNth * iDenominator // iNumerator for iNth in range(iNumerator)]
    iFilesInAll = 0
    iProcessed  = 0
    try:
        SubDirs = sorted([path for path in Path(sRootDir).glob('*') if path.is_dir()])
    except:
        sys.stderr.write("Failure listing files in root directory %s.  Perhaps you do not have permission to read from this directory?"
                %sRootDir)
        exit(1)
    #If we get here, we have permission to read the root directory.  (We'll check
    # each subdir below.)
    iSubdirs = len(SubDirs)
    for iNthDir, sSubdir in enumerate(SubDirs, start=1): #iNthDir used for status
        # message, so start=1 to make sense to non-computer scientists
        try:
            Files = sorted([sFName for sFName in sSubdir.glob('*.txt')],
                           key=lambda x: int(x.stem.lstrip('PMID')))
              #'PMID' means "PubMedID"
        except:
            sys.stderr.write("Failure listing files in directory %s.  Perhaps you do not have permission to read from this directory?"
                %sSubdir)
            exit(1)
        #If we get here, we have permission to at least read this directory
        for iNthFileInDir, sTxtFName in enumerate(Files):
            iFilesInAll += 1 #Overall number of files processed
            if iFilesInAll % iUpdateInterval == 0:
                sys.stderr.write("Processing directory {} of {}, file {}; {} files included so far.\r"
                                 .format(iNthDir, iSubdirs, iFilesInAll, iProcessed))
                sys.stderr.flush()
            if not (iFilesInAll % iDenominator in Intervals):
                continue #Skip this one
            #If we get here, we want to process this abstract file
            iProcessed += 1
            try:
                with sTxtFName.open('r', encoding='utf-8') as strTxtFile:
                    sData = strTxtFile.read().lower().strip()
                    if not sData:
                        sys.stderr.write("Found empty file %s\n" %sTxtFName)
                        continue
                    Tokens = regex.findall(r"[\w-]+", sData, flags=regex.VERSION1)
                    sPubMedID = sTxtFName.stem.lstrip('PMID')
                    strOut.write('{:08d} {}\n'.format(int(sPubMedID), ' '.join(Tokens)))
            except:
               sys.stderr.write("Failure processing file %s.  Perhaps you do not have read permission on this file?"
                   %sTxtFName)
               exit(1)
    sys.stderr.write("\n") #Retain last progress message on-screen



if __name__ == '__main__':
    (sRootDir, strOut, iNumerator, iDenominator) = GetCmdLineParameters()
    build_subset(sRootDir, iNumerator, iDenominator, strOut)
    strOut.close()
