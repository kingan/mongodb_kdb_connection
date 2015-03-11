#!/usr/local/bin/env python2.7
# -*- coding: utf-8 -*-

import time
from itertools import islice
import sys, os
from pickle import load
import xmlDictObject
from pymongo import MongoClient
import process_text
import nltk
from textblob import TextBlob
from textblob_aptagger import PerceptronTagger
import re
import datetime

#input = '2014-05_2.nml'
input = sys.argv[1]
f = open(input,'r')
opcl_tagset=[]
for (i,line) in enumerate(f):
    if '<doc ' in line:
        opcl_tagset.append(i)
    if '</doc>' in line:
        opcl_tagset.append(i)

f.seek(0)
incr_index = 0
final_list = []


def manip_name(input):
    input = input[:-4]
    input = input.split('/')
    input = input[-1:]
    input = input[0]
    input = input+'.xml'
    return input


input = manip_name(input)
st_input = input[:-4]

def append_final_list():
    global opcl_tagset
    global incr_index
    global final_list
    next_n_lines = []
    #The -3 should eliminate all XML declarations in the document
    opening_tag = opcl_tagset[incr_index]-3
    closing_tag = opcl_tagset[incr_index+1]
    n = closing_tag - opening_tag
    next_n_lines = list(islice(f, 2, n))
    incr_index+=2
    final_list.append(next_n_lines)

def well_formed_xml(well_formed_data):
    re_strip_pi = re.compile('<\?xml [^?>]+\?>', re.M)     #search pattern
    data = '<root>' + open(well_formed_data, 'r+').read() + '</root>'    #read in the file and wrap root tag around it
    match = re_strip_pi.search(data)                #find the locations of the matching lines
    data = re_strip_pi.sub('', data)                #strip the lines from the data
    re_strip_pi2 = re.compile('<\!DOCTYPE [^?>]+>', re.M)     #search another pattern
    data = re_strip_pi2.sub('', data)               #strip the lines from the data
    data = '<?xml version="1.0" encoding="ISO-8859-1" ?>\n'+ '<!DOCTYPE doc SYSTEM "djnml-1.0b.dtd">\n'  + data
    well_formed_data = well_formed_data[:-4]
    out = open(well_formed_data+'_stripped.xml', 'w')
    out.write(data)
    out.close()


def run_master():
    global final_list
    global input
    global st_input
    upd_string = ''
    print len(final_list)
    for n in range(0,len(final_list)):
        upd_string += ' '.join(final_list[n])
    out = open(input, 'w')
    out.write(upd_string)
    out.close()
    final_list = []


    #Generate well-formed XML
    well_formed_xml(input)
    #Create a Dictionary object with the XML tags removed
    configdict = xmlDictObject.ConvertXmlToDict(st_input+'_stripped.xml')
    #Processing of data && prepare import into Mongo
    #client = MongoClient('grv-infobright', 27017)
    client = MongoClient('grv-db01', 27017)
    db = client['djnNews']

    bulk = db.djnNews_final.initialize_unordered_bulk_op()
    stopwords = process_text.get_stopwords()

    for num in range(0,len(configdict['root']['doc'])):

        #Get text and processed text
        text_corpi = (configdict['root']['doc'][num]['djnml']['body']['text']).values()
        text = []
        for list_item in text_corpi:

            if isinstance(list_item, str):
                text.append(nltk.word_tokenize(list_item))
            elif isinstance(list_item, list):
                text.append(process_text.nltk_recursive_process_text(list_item))
        text_tokens = [item for sublist in text for item in sublist]
        text = " ".join(text_tokens)
        filtered_text = process_text.filtered_text(text_tokens,stopwords)
        blob = TextBlob(" ".join(filtered_text))
        tagged_text = blob.tags
        tagged_text = [(word,tag) for (word,tag) in tagged_text if tag != 'CD']
        filtered_text = [word for (word,tag) in tagged_text if tag != 'CD']
        #Needs to be done twice to remove all filtered text. Probably should adjust that later
        filtered_text = process_text.filtered_text(filtered_text,stopwords)

        text_bigrams, text_trigrams  = process_text.get_ngrammers(filtered_text)

        #Get headline and processed headline
        if type(configdict['root']['doc'][num]['djnml']['body']['headline']) in [str,unicode]:
            headline = (configdict['root']['doc'][num]['djnml']['body']['headline'])
        else:
            headline = (configdict['root']['doc'][num]['djnml']['body']['headline']['_text'])
        headline_tokens = headline.split()
        headline_blob = TextBlob(headline)
        headline_noun_phrases = headline_blob.noun_phrases
        filtered_headlines = process_text.filtered_text(headline_tokens, stopwords)
        headline_blob = TextBlob(" ".join(filtered_headlines))
        tagged_headline = headline_blob.tags
        filtered_headline = [word for (word,tag) in tagged_headline if tag != 'CD']
        tagged_headline = [(word,tag) for (word,tag) in tagged_headline if tag != 'CD']
        headline_bigrams, headline_trigrams = process_text.get_ngrammers(filtered_headline)


        processed_dict = {"filtered_headline":filtered_headline,"tagged_text":tagged_text,"tagged_headline":tagged_headline,"text_tokens":text_tokens,"text_bigrams":text_bigrams,"text_trigrams":text_trigrams,"headline_tokens":headline_tokens, "headline_bigrams":headline_bigrams, "headline_trigrams":headline_trigrams, "filtered_text":filtered_text}
        processed_dict.update({"headline_noun_phrases":headline_noun_phrases})

        #Get the date from display-date column
        try:
            date = (configdict['root']['doc'][num]['djnml']['head']['docdata']['djn']['djn-newswires']['djn-mdata']['display-date']).split('T')[0]
        except:
            date = ''

        #Get the relevant symbols from the nested list
        djnnews = configdict['root']['doc'][num]['djnml']['head']['docdata']['djn']['djn-newswires']

        stripped_levels_list = []
        stripped_levels_list = process_text.recursive_list(djnnews,stripped_levels_list)
        sub_list = [item for item in stripped_levels_list if item[0] == 'c']
        for sub_list_num in range(0,len(sub_list)):
            sub_list[sub_list_num][0] = djnnews['djn-mdata']['djn-coding'].keys()[sub_list_num]

        stripped_levels_list = [item for item in stripped_levels_list if item not in sub_list]
        dict_djnnews = {}
        try:
            dict_djnnews = dict(stripped_levels_list)
            dict_djnnews.update({'relevantSyms': dict(sub_list)})
            dict_djnnews.update({"processed_text":processed_dict})
            dict_djnnews.update({"text":text,"headline":headline})
            dict_djnnews.update({"date":date})
        except:
            print "Failed to parse data"
        #Import into Mongo
        bulk.insert(dict_djnnews)
    try:
        bulk.execute()
    except:
        print "Failed to execute bulk insert."
    del djnnews, dict_djnnews, sub_list, stripped_levels_list


def main():
    #Run the scripts
    #for num in range(0,10000):
    for num in range(0,len(opcl_tagset)/2):
        if (num+1) % 5000 != 0:
            append_final_list()
        else:
            append_final_list()
            #Execute on 5,000 records
            run_master()
            print "Output"
    #Run again to execute function on remaining records
    run_master()

    #Remove the temporary files
    os.remove(input)
    os.remove(st_input+'_stripped.xml')


if __name__ == "__main__":
    tic = time.clock()
    main()
    toc = time.clock()
    print(toc - tic)

