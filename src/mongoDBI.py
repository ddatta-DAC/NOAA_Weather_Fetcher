import pymongo
from pymongo import MongoClient
import cPickle
from bson.binary import Binary
import numpy as np
import os
import sys
import constants


class mongoDBI:
    conn = None
    db_name = None

    # start_db_cmd = 'sudo service mongod start'

    def __init__(self, db_name):
        # DEBUG:  Run this to start mongodb
        # os.system(self.start_db_cmd)
        self.db_name = db_name
        self.conn = MongoClient ()

    def insert_obj(self, table, key_label, key_contents, value_label, value_contents):
        var_obj = ".".join ([ "self.conn", self.db_name, table ])
        collection = eval (var_obj)
        dict = self.get_insert_dict (key_label, key_contents, value_label, value_contents)
        try:
            collection.insert (dict);
        except:
            print('Error in inserting to db. Table ' + table)

        return;

    def find(self, table, key_label, key_contents, value_label):
        var_obj = ".".join ([ "self.conn", self.db_name, table ])
        collection = eval (var_obj)
        k = collection.find_one ({key_label: key_contents})

        if k is None:
            return None

        result = cPickle.loads (k[ value_label ])
        return result;

    # Return a dict such that it can be used in insert or insert_many
    # _id makes the record unique
    @staticmethod
    def get_insert_dict(key_label, key_contents, value_label, value_contents):
        p_value = Binary (cPickle.dumps (value_contents))
        dict = {constants.id_key: key_contents,
                key_label: key_contents,
                value_label: p_value}
        return dict


    def find_last(self, table):
        var_obj = ".".join ([ "self.conn", self.db_name, table ])
        collection = eval (var_obj)
        for p in collection.find ().sort ("id", pymongo.ASCENDING):
            print (p)
        return;

    # ----------------------------------------------------------------#
    # IMPLEMENTS UPDATE
    # Input dict { 'table' : list of data_dict, ... }
    # Each data_dict is obtained by calling get_insert_dict
    #
    def insert_bulk(self, map_table_data):

        for  table,list_dict in map_table_data.iteritems ():

            if len (list_dict) == 0:
                continue;

            var_obj = ".".join ([ "self.conn", self.db_name, table ])
            collection = eval (var_obj)

            # Check if already present , if so delete
            for element in list_dict:
                try:
                    collection.remove ({constants.id_key: element[ constants.id_key ]})
                except:
                    pass

            try:
                collection.insert_many (list_dict)
            except:
                print (" Failed to write to mongoDB! ")
        return;



        # ----------------------------------------------------------------#

# mdb.insert_obj( "rh" , '_id', "2012-10-12", 'data' , np.random.rand(5,2));
# mdb.insert_obj( "rh" , '_id', "2013-10-12", 'data' , np.random.rand(5,6));
# r = mdb.find( "rel_hum", 'id' ,'def','data');
# mdb.find_last()
