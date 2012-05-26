"""
MapReduce job to convert a set of raw exodus files to corresponding sequence files.
The mapper just reads the file name, fetches the file and converts it into a set of 
sequence files.

example: 
python mr_exodus2seq_local.py input.txt -t 30 -d /home/hou13/Codes/testmrjob/ --variables TEMP
"""

__author__ = 'Yangyang Hou <hyy.sun@gmail.com>'

import sys
import os
from mrjob.job import MRJob

import exopy2 as ep
from hadoop.io import SequenceFile
from hadoop.io.SequenceFile import CompressionType
from hadoop.typedbytes import *

def convert(inputfile, steps, outdir, variables):
    f = ep.ExoFile(inputfile,'r')
    total_time_steps = f.num_time_steps

    if total_time_steps < steps:
        print >> sys.stderr, 'The total time steps is', total_time_steps
        print >> sys.stderr, 'The patitions step is',steps,'. No need to patition the file.'
        return False
        
    Vars = variables.split(',')
    
    # Get time data and coordinate (x,y,z) data
    time = f.cdf.variables["time_whole"]
    timedata = time.getValue()
    coordz = f.cdf.variables["coordz"]
    zdata = coordz.getValue()
    coordy = f.cdf.variables["coordy"]
    ydata = coordy.getValue()
    coordx = f.cdf.variables["coordx"]
    xdata = coordx.getValue()
    
    # Get variable data
    varnames = f.node_variable_names()
    vardata=[]
    for i, var in enumerate(Vars):
        vdata = None
        for vi,n in enumerate(varnames):
            if n == var.strip():
                #vtemp = vi
                vindex = vi
                break
        if vindex == None:
            print  >> sys.stderr, 'The variable ', var.strip(), 'does not exist!'
            return False
        tmp = f.vars['vals_nod_var'+str(vindex+1)]
        tmpdata = tmp.getValue()
        vardata.append((var.strip(), tmpdata))
    
    # Begin to partition
    basename = os.path.basename(inputfile)
    ind = basename.rfind('.')
    basename = basename[0:ind]
    
    outdir = os.path.join(outdir,basename)
    os.mkdir(outdir)
    
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
            valuedata=[]
            for m, var in enumerate(vardata):
                name = var[0]
                data = var[1][j]
                valuedata.append((name,data))
            value.set(valuedata)
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
    
    return True
    
    
class MRExodus2Seq(MRJob):

    def configure_options(self):
        """Add command-line options specific to this script."""
        super(MRExodus2Seq, self).configure_options()
        
        self.add_passthrough_option(
            '-t', '--timesteps', dest='timesteps',
            type='int',
            help='-t NUM or --timesteps NUM, Groups the output into batches of NUM timesteps'       
        )
        self.add_passthrough_option(
            '-d', '--outdir', dest='outdir',
            help='-d DIR or --outdir DIR, Write the output to the directory DIR'
        )
        self.add_passthrough_option(
            '--variables', dest='variables',
            help='--variables VARS, Only output the variables in the comma delimited list'       
        )
       
        
    def load_options(self, args):
        super(MRExodus2Seq, self).load_options(args)
        
        if self.options.timesteps is None:
            self.option_parser.error('You must specify the --timesteps NUM or -t NUM')
        else:
            self.timesteps = self.options.timesteps
            
        if self.options.outdir is None:
            self.option_parser.error('You must specify the --outdir DIR or -d DIR')
        else:
            self.outdir = self.options.outdir
            
        if self.options.variables is None:
            self.option_parser.error('You must specify the --variables VARS')
        else:
            self.variables = self.options.variables
       
    
    def mapper(self, _, line):
        result = convert(line, self.timesteps, self.outdir, self.variables)        
        if result == True:
            yield (line, 0)
        else:
            yield (line, 1)

    def reducer(self, key, values):
        yield (key, sum(values))
        
    def steps(self):
        return [self.mr(self.mapper, self.reducer),]

if __name__ == '__main__':
    MRExodus2Seq.run()
