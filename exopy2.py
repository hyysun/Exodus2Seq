#!/usr/bin/env python

import sys
import os

import numpy as np
try:
    import Scientific.IO.NetCDF as netcdf
except ImportError:
    import pynetcdf as netcdf    

"""
exopy2.py
=========
"""

__author__ = 'David F. Gleich'

class ExoFile(object):
    """ A low-level interface to an exodus file.  This class is
    a somewhat thin-wrapper around the NetCDFFile class, that stores
    many of the constants that the exodus datatype uses. """
    def __init__(self,filename,attrs='r'):
        """ 
        @param filename the name of the exodus file 
        @param attrs the file type attributes:
            'r' - read only (filename must exist)
            'a' - append/new file
            'w' - new file (erases any existing contents)
        """
        self.filename = filename
        self.cdf = netcdf.NetCDFFile(filename,attrs)
        self.dims = self.cdf.dimensions
        self.vars = self.cdf.variables
        
    def _maxlines(self,list,n):
        """ A utility routine to help show a synopsis of a exodus file. """
        if len(list) > n:
            list = list[0:n]
            list.append('[... truncated to %i lines ...]'%(n))
        return list
        
    def __repr__(self):
        """ Show a textual representation of the file data. """
        s = "exodus ii file:\n"
        s += "  filename: %s\n"%(self.filename)
        s += "  title: '%s'\n"%(self.title)
        s += "  version: %.f\n"%(self.version)
        s += "  api_version: %.f\n"%(self.api_version)
        s += "  floating_point_word_size: %i\n"%(self.floating_point_word_size)
        s += "  file_size: %i\n"%(self.file_size)
        s += "  dimensions:\n"
        s += "    num_nodes: %i\n"%(self.num_nodes)
        s += "    num_elem: %i\n"%(self.num_elem)
        s += "    num_dim: %i\n"%(self.num_dim)
        s += "    num_nod_var: %i\n"%(self.num_nod_var)
        s += "    num_time_steps: %i\n"%(self.num_time_steps)
        s += "  coordinates:\n"
        for name in self._maxlines(self.coordinate_names(),5):
            s += "    - '%s'\n"%(name)
        s += "  node variables:\n"
        for name in self._maxlines(self.node_variable_names(),10):
            s += "    - '%s'\n"%(name)
        s += "  info records:\n"
        for line in self._maxlines(self.info_records(),10):
            s += "    - '%s'\n"%(line)
        s += "  qa records:\n"
        for rec in self.qa_records():
            s += "    - \n"
            for line in rec:
                s += "      - '%s'\n"%(line)
        return s
        
    #property(fset=set_title, fget=get_title, doc="The exodus database title")
        
    def _str_property(field,doc):
        """ A function to generate data for a string property. """
        def fget(self):
            return getattr(self.cdf,field)
        def fset(self,val):
            assert(type(val) == type(str))
            setattr(self.cdf,field,val)
        prop = locals()
        del prop['field']
        return prop
        
    def _float_property(field,doc,default=None):
        def fget(self):
            return float(getattr(self.cdf,field,default)[0])
        def fset(self,val):
            assert(type(val) == type(float))
            setattr(self.cdf,field,np.array(val,dtype=np.float32))
        prop = locals()
        del prop['field']
        del prop['default']
        return prop
        
    def _int_property(field,doc,default=None):
        def fget(self):
            return int(getattr(self.cdf,field,default)[0])
        def fset(self,val):
            assert(type(val) == type(int))
            setattr(self.cdf,field,np.array(val,dtype=np.int32))
        prop = locals()
        del prop['field']
        del prop['default']
        return prop
        
    # EXODUS II global file properties
    title = property(**_str_property("title",doc="The exodus database title"))
    version = property(**_float_property("version",
        doc="The exodus II file version number"))
    api_version = property(**_float_property("api_version",
        doc="The exodus II API version number"))
    floating_point_word_size = property(
        **_int_property("floating_point_word_size",
        doc="word size of floating point numbers in the file"))
    file_size = property(**_int_property("file_size",
        doc="The database format", default=0))
        
    def _dims_property(field,doc):
        """ A helper function for dimension based properties. """
        def fget(self):
            return int(self.dims[field])
        prop = locals()
        del prop['field']
        return prop
        
    # dimension properties
    num_nodes = property(**_dims_property("num_nodes",
        doc="number of nodes in the model"))
    num_dim = property(**_dims_property("num_dim",
        doc="number of nodes in the model"))
    num_elem = property(**_dims_property("num_nodes",
        doc="number of nodes in the model"))
    num_glo_var = property(**_dims_property("num_glo_var",
        doc="number of nodes in the model"))
    num_nod_var = property(**_dims_property("num_nod_var",
        doc="number of nodes in the model"))
    
    len_string = property(**_dims_property("len_string",
        doc="length of string types"))
        
    # the following are all information records
    num_info = property(**_dims_property("num_info",
        doc="number of information lines"))
    num_qa_rec = property(**_dims_property("num_qa_rec",
        doc="number of qa lines"))
    num_qa_strings = property(**_dims_property("four",
        doc="number of strings in a single QA record"))
    
    
    def get_num_time_steps(self):
        """ Return the total number of time steps. """
        return len(self.vars['time_whole'].getValue())
        
    num_time_steps = property(fget=get_num_time_steps,doc="the number of time steps")
    
    def time_steps(self):
        return self.vars['time_whole'].getValue()
    
    def global_variables_names(self):
        names = self.vars['name_glo_var'].getValue()
        return [n.tostring().rstrip('\x00') for n in names]
        
    def node_variable_names(self):
        names = self.vars['name_nod_var'].getValue()
        return [n.tostring().rstrip('\x00') for n in names]
        
    def coordinate_names(self):
        namedata = self.vars['coor_names'].getValue()
        names = [n.tostring().rstrip('\x00') for n in namedata]
        assert(len(names) == self.num_dim)
        return names
        
    def qa_records(self):
        recdata = self.vars['qa_records'].getValue()
        recs = []
        for i in xrange(self.num_qa_rec):
            r = []
            for j in xrange(self.num_qa_strings):
                r.append(recdata[i][j].tostring().rstrip('\x00'))
            recs.append(r)
        return recs
    
    def info_records(self):
        infodata = self.vars['info_records'].getValue()
        info = [d.tostring().rstrip('\x00') for d in infodata]
        assert(len(info) == self.num_info)
        return info

if __name__=='__main__':
    pass
    # setup some sort of test case
