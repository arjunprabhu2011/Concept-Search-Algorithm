import os, os.path
import json
from pickle import TRUE
from urllib.request import Request, urlopen
import urllib.request
from regex import P
import requests
from whoosh.fields import Schema, TEXT, KEYWORD, ID, STORED
from whoosh import index
import codecs
import fnmatch
from whoosh.qparser import QueryParser
import PyPDF2
import fitz
from whoosh.highlight import SentenceFragmenter
import pandas as pd

json_filepath = 'cs-documents.json'

json_file = open(json_filepath)

data = json.load(json_file)

concept_terms = pd.read_csv("Keywords-Springer-83K-20210405.csv")

def parse_documents(search_term):
    schema = Schema(identification=ID(stored=True),
                    authors=TEXT(stored=True),
                    title=TEXT(stored=True),
                    content=TEXT(stored=True)
                    )

    if not os.path.exists("indexdir"):
        os.mkdir("indexdir")

    ix = index.open_dir("indexdir")
    writer = ix.writer()
    
    # for doc in data["documents"]:
    #     #Ignore hidden files
    #     curr_identification = doc["id"]
    #     curr_authors = doc["authors"]
    #     curr_title = doc["title"]
    #     curr_content = doc["abstract"]

    #     print(curr_identification)

    #     writer.add_document(identification=curr_identification, authors=curr_authors, title=curr_title, content=curr_content)

    # writer.commit()

    #SEARCHING
    with ix.searcher() as searcher:
        parser = QueryParser("content", ix.schema)
        myquery = parser.parse(search_term)
        sf = SentenceFragmenter(charlimit=100000)
        results = searcher.search(myquery, limit=200000)
        results.fragmenter = sf

        all_sentences = []

        for hit in results:
            # print(hit["title"])
            # print(hit["identification"])
            total_string_for_hit = hit.highlights("content")
            curr_sentence_array = total_string_for_hit.split("...")
            if (curr_sentence_array == ['']):
                continue

            #print(curr_sentence_array)
            all_sentences.append(curr_sentence_array)

        #print(len(all_sentences))

    term_column = concept_terms.iloc[:,1]
    concept_term_score_dict = {}
    snippet_dict = {}
    
    for term in term_column:
        num_times_in_all_sentences = 0
        total_proximity = 0
        snippet = "" 
        first_sentence = True
        sentence_num = 0
        for sent_array in all_sentences:
            if (sentence_num >= 100):
                break 
            for sentence in sent_array:
                if (sentence_num >= 100):
                    break

                sentence = sentence.replace('<b class="match term0">', '')
                sentence = sentence.replace('</b>', '')
                words_in_sentence = sentence.split()
                exact_term_one = " " + term + " "

                if (((exact_term_one in sentence)) and (search_term in words_in_sentence)):
                    if (first_sentence == True):
                        snippet = sentence
                        first_sentence = False

                    if " " in term:
                        term_parts = term.split()
                        total_proximity += min(abs(words_in_sentence.index(term_parts[0]) - words_in_sentence.index(search_term)), abs(words_in_sentence.index(term_parts[len(term_parts) - 1]) - words_in_sentence.index(search_term)))
                    else:
                        total_proximity += abs(words_in_sentence.index(term) - words_in_sentence.index(search_term))

                    num_times_in_all_sentences += 1

                sentence_num += 1
         
        if (total_proximity > 0):
            average_proximity = total_proximity / num_times_in_all_sentences * 1.0
            concept_term_score_dict[term] = num_times_in_all_sentences * 2.0 + 1.0 / average_proximity
        else:
            average_proximity = 0.0
            concept_term_score_dict[term] = 0

        snippet_dict[term] = snippet

    sorted_score_dict = dict(sorted(concept_term_score_dict.items(), key=lambda item: item[1]))
    term_one = list(sorted_score_dict.keys())[-1]
    term_two = list(sorted_score_dict.keys())[-2]
    term_three = list(sorted_score_dict.keys())[-3]
    print(term_one + ": " + snippet_dict[term_one] + "\n")
    print(term_two + ": " + snippet_dict[term_two] + "\n")
    print(term_three + ": " + snippet_dict[term_three] + "\n")
    # print(concept_term_score_dict[term_one])
    # print(concept_term_score_dict[term_two])
    # print(concept_term_score_dict[term_three])
    # print(snippet_dict[term_one])
    # print(snippet_dict[term_two])
    # print(snippet_dict[term_three])

parse_documents("data")

