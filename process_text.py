import sys, os, pymongo, nltk, pprint, numpy, bson
from nltk.corpus.reader.plaintext import PlaintextCorpusReader
from nltk.util import ngrams
from datetime import datetime, date, time, timedelta

def get_stopwords():
    stopwords = nltk.corpus.stopwords.words('english')
    more_stopwords = [u'a', u'able', u'about', u'across', u'after', u'all', u'almost', u'also', u'am', u'among', u'an', u'and', u'any', u'are', u'as', u'at', u'be', u'because', u'been', u'but', u'by', u'can', u'cannot', u'could', u'dear', u'did', u'do', u'does', u'either', u'else', u'ever', u'every', u'for', u'from', u'get', u'got', u'had', u'has', u'have', u'he', u'her', u'hers', u'him', u'his', u'how', u'however', u'i', u'if', u'in', u'into', u'is', u'it', u'its', u'just', u'least', u'let', u'like', u'likely', u'may', u'me', u'might', u'most', u'must', u'my', u'neither', u'no', u'nor', u'not', u'of', u'off', u'often', u'on', u'only', u'or', u'other', u'our', u'own', u'rather', u'said', u'say', u'says', u'she', u'should', u'since', u'so', u'some', u'than', u'that', u'the', u'their', u'them', u'then', u'there', u'these', u'they', u'this', u'tis', u'to', u'too', u'twas', u'us', u'wants', u'was', u'we', u'were', u'what', u'when', u'where', u'which', u'while', u'who', u'whom', u'why', u'will', u'with', u'would', u'yet', u'you', u'A', u'Able', u'About', u'Across', u'After', u'All', u'Almost', u'Also', u'Am', u'Among', u'An', u'And', u'Any', u'Are', u'As', u'At', u'Be', u'Because', u'Been', u'But', u'By', u'Can', u'Cannot', u'Could', u'Dear', u'Did', u'Do', u'Does', u'Either', u'Else', u'Ever', u'Every', u'For', u'From', u'Get', u'Got', u'Had', u'Has', u'Have', u'He', u'Her', u'Hers', u'Him', u'His', u'How', u'However', u'I', u'If', u'In', u'Into', u'Is', u'It', u'Its', u'Just', u'Least', u'Let', u'Like', u'Likely', u'May', u'Me', u'Might', u'Most', u'Must', u'My', u'Neither', u'No', u'Nor', u'Not', u'Of', u'Off', u'Often', u'On', u'Only', u'Or', u'Other', u'Our', u'Own', u'Rather', u'Said', u'Say', u'Says', u'She', u'Should', u'Since', u'So', u'Some', u'Than', u'That', u'The', u'Their', u'Them', u'Then', u'There', u'These', u'They', u'This', u'Tis', u'To', u'Too', u'Twas', u'Us', u'Wants', u'Was', u'We', u'Were', u'What', u'When', u'Where', u'Which', u'While', u'Who', u'Whom', u'Why', u'Will', u'With', u'Would', u'Yet', u'You']

    stopwords = stopwords + more_stopwords
    stopwords = sorted(set(stopwords))
    punctuation = [u"[",u"]",u".",u"'",u" ", u",",u":",u"(",u")",u"^^",u"^",u"^^^",u"\\",u"-",u"",u"--",u"?",u"@",u"``",u"*",u"**",u"***",u"|",u"=",u"==",u"+",u"&",u"_"]
    stopwords = stopwords + punctuation
    return stopwords


def recursive_process_text(l):
    final_output = ''
    for item in l:
        if isinstance(item,list):
            recursive_process_text(item)
        else:
            final_output += " " + item
    final_output = final_output.split()
    return final_output

def nltk_recursive_process_text(l):
    final_output = ''
    for item in l:
        if isinstance(item,list):
            recursive_process_text(item)
        else:
            final_output += " " + item
    final_output = nltk.word_tokenize(final_output)
    return final_output


def recursive_strip_nested(arv):
    i = 0
    for item in arv.values():
        if isinstance(item, dict):
           recursive_strip_nested(item)
           i += 1
        else:
           final_list.update({arv.keys()[i]:item})
           i += 1
    return final_list


def recursive_list(arv,stripped_levels_list):
    recursive_list_index = 0
    for item in arv.values():
        if isinstance(item, dict):
            recursive_list(item,stripped_levels_list)
            recursive_list_index +=1
        else:
            stripped_levels_list.append([arv.keys()[recursive_list_index],item])
            recursive_list_index += 1
    return stripped_levels_list


def filtered_text(text_tokens,stopwords):
    filtered_text = [w for w in text_tokens if not w in stopwords]
    #freq_text = nltk.FreqDist(filtered_text)
    #freq_text = freq_text.most_common()
    return filtered_text #,freq_text)


def ngrammer(text,n):
    split = ngrams(text,n)
    g = []
    for grams in split:
        g.append(grams)
    return g


def get_ngrammers(text_tokens):
    text_bigrams = ngrammer(text_tokens,2)
    text_trigrams = ngrammer(text_tokens,3)

    #freq_text_bigrams = nltk.FreqDist(text_bigrams)
    #freq_text_bigrams = freq_text_bigrams.most_common()
    #freq_text_trigrams = nltk.FreqDist(text_trigrams)
    #freq_text_trigrams = freq_text_trigrams.most_common()

    return(text_bigrams, text_trigrams) #, freq_text_bigrams, freq_text_trigrams)
