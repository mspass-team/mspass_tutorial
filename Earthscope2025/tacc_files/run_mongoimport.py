#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 19 17:44:00 2025

@author: pavlis
"""

import subprocess

def make_import_command_line(collection,db="usarray48",dir=None)->list:
    """`
    Returns the command line needed to import a specified 
    collection.   db is teh database name to use which defaultls 
    to "usarray48" which is the name used in the Earthscope2025 
    course.  Returns a list that can be passed to subproces.run
    """
    runline = ["mongoimport"]
    token="--db={}".format(db)
    runline.append(token)
    token="--collection={}".format(collection)
    runline.append(token)
    if dir:
        fname = dir + "/{}.json".format(collection)
    else:
        fname = collection + ".json"
    token="--file={}".format(fname)
    runline.append(token)
    return runline

# test
#x=make_import_command_line("source")
#print(x)
#x=make_import_command_line("channel",dir="/foo/bar")
#print(x)
dir="./exports"   # likely to change for testing
for collection in ["source","site","channel"]:
    runline=make_import_command_line(collection,dir=dir)
    try:
        result = subprocess.run(runline,
                                capture_output=True, 
                                text=True, 
                                check=True,
                                )
        print("Standard Output importing collection-",collection)
        print(result.stdout)
        print("Standard Error from same:")
        print(result.stderr)
        print(f"Return Code: {result.returncode}")

    except subprocess.CalledProcessError as e:
        print(f"Error executing command: {e}")
        print(f"Standard Output: {e.stdout}")
        print(f"Standard Error: {e.stderr}")
    