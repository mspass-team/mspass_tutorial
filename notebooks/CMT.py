#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
This module contains a set of utilities to parse the archaic so 
called "ndk" format text files used to distribute the Global 
Centroid Moment Tensr catalog.  ndk is an obnoxious text format 
with obvious archaic roots to the dark ages of punched cards.  
It uses 5, 80-column lines for each earthquake in the catalog. 
That type of data is very difficult to parse in python, but 
that is what this does.   

Created on Wed Jan  3 06:03:05 2024

@author: pavlis
"""

def read_ndk_file(path,apply_checks=True):
    """
    """
    fh = open(path,'r')
    alllines = fh.readlines()
    fh.close()
    # This is a sanity check.  Every third line in the full catalog 
    # I have has this magic string.  The format description doesn't 
    # state this as a requirement, but it seems to be so.
    # If you get this message you may want to turn off default check
    if apply_checks:
        if len(alllines)%5 != 0:
            message = "read_ndk_file: file {} appears to be corrupted\n".format(path)
            message += "File has {} lines which is not a multiple of 5 that is propertly of ndk files\n".format(len(alllines))
            raise RuntimeError(message)
        i=2
        while i<len(alllines):
            testline=alllines[i]
            teststring=testline[0:8]
            if teststring != "CENTROID":
                message = "read_ndk_file: file {} appears to be corrupted at line {}\n".format(path,i)
                message += "Line content:  [{}]".format(testline)
                message += "That line should contain the magic string CENTROID"
                raise RuntimeError(message)
            i += 5
    return alllines

def parse_ndk_image(lines):
    """
    Parses the binary image of an ndk file read with the read_ndk_file 
    function.   That means it assumes arg0 is a list of strings 
    from the formatted ndk text file.  The algorithm assumes the 
    file line count is a multiple of 5, which is specified by the 
    archaic ndk file structure.   
    
    
    Returns a python dict with keys for attributes set as constants 
    in this function.  That effectively imposes a schema definition 
    that could be used with MongoDB but would require coordination 
    with the constants in this function.
    """
    # These tuples define the format for each line and the key 
    # to which each value should be associated
    # each tuple is start, end, key, type (s,f,i for string, float, and int)  
    # date strings have to be treated specially
    form_def = []
    # the document on this format is wrong on some of these fields
    # I think the riginal format may have dropped the decimal points for 
    # float values  Also ms and mb fields are not properly defined
    # had to infer this
    f = [
          [0,3,'location_source','s'],
          [4,14,'date','s'],
          [16,25,'time_of_day','s'],
          [27,33,'lat','f'],
          [34,41,'lon','f'],
          [42,47,'depth','f'],
          [48,51,'mb','f'],
          [52,54,'Ms','f'],
          [56,79,'geographic_comment','s']
        ]
    form_def.append(f)
    # line 2
    f = [
            [0,15,'CMT_event','s'],
            [17,60,'CMT_data_used','s'],
            [62,67,'invesion_type','s'],
            [69,79,'moment_rate_function','s']
        ]
    form_def.append(f)
    # line 3
    f = [
            [0,57,'centroid','s'],  # this token will need to be split into multiple attributes
            [59,61,'depth_type','s'],
            [64,79,'timestamp','s']
        ]
    form_def.append(f)
    # line 4
    f = [
            [0,1,'exponent','i'],
            [2,79,'MT_components','s'],  # will definitely need to be split up to be useful
        ]
    form_def.append(f)
    # line 5
    f = [
            [0,2,'version','s'],
            [3,47,'MT_pc','s'],
            [49,55,'moment','f'],
            [57,79,'sdr','s'], # strike-dip-rake needs to be split
        ]
    form_def.append(f)
    
    # loop over the list of lines 5 at a time. 
    # range increment makes this simpler
    doclist=[]
    for l0 in range(0,len(lines),5):
        doc = dict()
        # i runs over line number but ii run 0 to 4
        ii = 0
        for i in range(l0,l0+5):
            s = lines[i]
            f = form_def[ii]
            for j in range(len(f)):
                #print(l0,i,ii,j)
                start = f[j][0]
                end = f[j][1]
                key = f[j][2]
                type_def = f[j][3]
                sval = s[start:end]
                # too bad python doesn't have a swtich-case as that would be used here
                if type_def == 's':
                    val = sval
                elif type_def == 'f':
                    val = float(sval)
                elif type_def == 'i':
                    val = int(sval)
                else:
                    raise ValueError("Illegal value for type in foraat="+type_def)
                doc[key] = val
            ii += 1  
        doclist.append(doc)
        
    return doclist
        
# test with data file - path is frozen for this test but for notebook 
# will be set with download
fname = '/Users/pavlis/tutorials/mspass_tutorial/notebooks/jan76_dec20.ndk'
lines = read_ndk_file(fname)
doclist=parse_ndk_image(lines)
for i in range(5):
    print(doclist[i]['mb'],doclist[i]['Ms'])
