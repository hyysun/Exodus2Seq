#!/usr/bin/env python

import sys
import os

import exopy2 as ep
from hadoop.io import SequenceFile
from hadoop.io.SequenceFile import CompressionType
from hadoop.typedbytes import *

if __name__ == "__main__":
    """ 
    Convert a raw exodus file into a set of typedbytes sequence files
    """

    if len(sys.argv) != 4:
        print 'Usage: python', sys.argv[0], '<timesteps> <inputfile> <outdir>'
        sys.exit()
    """
    Args:
        timesteps: timesteps
        inputfile: the raw exodus file you want to partition
        outdir: directory to store typedbytes sequence files and index file
    """

    steps = int(sys.argv[1])
    inputfile = sys.argv[2]
    outdir = sys.argv[3]  # outdir must exist already, indexfile and sequence files would be stored here

    f = ep.ExoFile(inputfile,'r')
    total_time_steps = f.num_time_steps

    if total_time_steps < steps:
        print 'The total time steps is', total_time_steps
        print 'The patitions step is',steps,'. No need to patition the file'
        sys.exit()
    
    # Get time data and coordinate (x,y,z) data
    time = f.cdf.variables["time_whole"]
    timedata = time.getValue()
    coordz = f.cdf.variables["coordz"]
    zdata = coordz.getValue()
    coordy = f.cdf.variables["coordy"]
    ydata = coordy.getValue()
    coordx = f.cdf.variables["coordx"]
    xdata = coordx.getValue()
    
    # Get temperature data
    varnames = f.node_variable_names()
    vtemp = None
    for vi,n in enumerate(varnames):
        if n == 'TEMP':
            vtemp = vi
            break
    assert(vtemp is not None)
    temp = f.vars['vals_nod_var'+str(vtemp+1)]
    tempdata = temp.getValue()
    
    # Begin to partition
    basename = os.path.splitext(inputfile)[0]
    
    indexkey = TypedBytesWritable()
    indexvalue = TypedBytesWritable()
    indexwriter = SequenceFile.createWriter(os.path.join(outdir,'index.seq'), 
        TypedBytesWritable, TypedBytesWritable,compression_type=CompressionType.RECORD)
    
    begin = 0
    i = 0
    
    while begin < total_time_steps:
        end = begin + steps - 1
        if end > total_time_steps - 1:
            end = total_time_steps - 1
        outputfilename = basename + '_part'+ str(i) + '.seq'
        
        writer = SequenceFile.createWriter(os.path.join(outdir,outputfilename),
            TypedBytesWritable, TypedBytesWritable,compression_type=CompressionType.RECORD)
        key = TypedBytesWritable()
        value = TypedBytesWritable()
        key.set(-1)
        value.set(xdata)
        writer.append(key,value)
        key.set(-2)
        value.set(ydata)
        writer.append(key,value)
        key.set(-3)
        value.set(zdata)
        writer.append(key,value)
        
        for j in xrange(begin, end+1):
            key.set((j,timedata[j]))
            value.set(tempdata[j])
            writer.append(key,value)
        writer.close()
        indexkey.set(outputfilename)
        indexvalue.set(end-begin+1)
        indexwriter.append(indexkey,indexvalue)
        begin = begin + steps
        i = i + 1
        
    indexkey.set('total')
    indexvalue.set(total_time_steps)
    indexwriter.append(indexkey,indexvalue)   
    indexwriter.close()