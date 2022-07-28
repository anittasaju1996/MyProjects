# To get processing time of each cell
#% load_ext autotime

# !pip install -q transformers
# !pip install transformers[tf-cpu]
# !pip install ipython-autotime
# !pip install langdetect

# Import requiered libraries
from bs4 import BeautifulSoup
import datetime as dt
import requests
import re
import pandas as pd
import nltk
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet
from collections import Counter
import math
from langdetect import detect
from transformers import pipeline
from multiprocessing import Pool, Manager
import threading
import sys
import time
#import tensorflow
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

#### Error handling Info ###
#As the process takes a lot of time to run, to preseve atleast info that is gathered, most of the errors will only populate blank in the df.
#the main errors where the script will fail are :
#If not able to access main url and its respective page continuations
#If not able to access drama urls


class ModelFailureException(Exception):
    pass

# Function is called to increase function performace throught threading
# It splits the passed list into parts and runs different threads with the same function and arguments but different parts of list
# IP : Full lst to be processed, function name, extra arguments to be passed
# Exists after all the threads are complete
# Error  : handled in parent
def function_threading(lst, f, arguments):
    lock = threading.Lock()
    no_of_threads = 20 #Limiting the number of threads to start parallely
    lst_len = math.ceil(len(lst) / no_of_threads)
    threads = []

    print(f"Length of list passed: {lst_len}")

    #deciding the start and end index of lst to be passed
    for lst_start_index in range(0, len(lst), lst_len):
        if lst_start_index + lst_len >= len(lst):
            lst_end_index = len(lst)
        else:
            lst_end_index = lst_start_index + lst_len

        fun_arguments = (lst[lst_start_index:lst_end_index], lock) + arguments
        t = threading.Thread(target=f, args=fun_arguments)
        threads.append(t) #storing thread to check its completion later
        t.start()

    print("All threads started")

    for t in threads:
        t.join()

    print("All threads completed")


# Function to fetch name and link from the page
# Input : Beautiful Soup html , a dicitonary to update the name and link found
# Output : No return, info is stored in the passed dict
# Error : print msg but continue with rest
def get_link_mapping(doc, link_dict):
    lst_of_link_tags = doc.find_all("h6", class_="text-primary title")
    for i in lst_of_link_tags:
        try:
            if i.a.text.lower() in link_dict:
                key_word = i.a.text.lower() + "-"
                no = len(re.findall(key_word, ", ".join(link_dict.keys())))
                drama_name = key_word + str(
                    no + 1)  # In case two or more dramas have same name like descendants of the sun
            else:
                drama_name = i.a.text.lower()

            link_dict[drama_name] = site_url + i.a["href"]
        except Exception as e:
            print(f"WARN : {i.a.text.lower()} has no link!, error is : " + str(e))  # o/p to console in case of any errors


# Function called to iterating through pages passed and getting all the drama urls
def link_parsing(page_range, lock, link_mapping):
    for page_index in page_range:
        page_url = main_url + f"&page={page_index}"
        try:
            page_text = requests.get(page_url).text
            page_doc = BeautifulSoup(page_text, "html.parser")
        except Exception as e:
            print(f"ERROR : Not able to parse page no : {page_index} of the main page, error is : " + str(e))
            raise Exception("Not able to parse page for link")

        lock.acquire()
        get_link_mapping(page_doc, link_mapping)  # Calling function to get all drama urls for respective page
        lock.release()


# Function called to iterating through pages passed and getting all the reviews
def review_parsing(review_page_range, lock, review_url, all_reviews, reviewer_profile_links):
    for page_index in review_page_range:
        page_url = review_url + f"&page={page_index}&sort=helpful"
        try:
            page_text = requests.get(page_url).text
            page_doc = BeautifulSoup(page_text, "html.parser")
            # print(page_index, end=" ")
        except Exception as e:
            print(f"ERROR : Not able to parse page no : {page_index} of the reviews page, error is : " + str(e))
            raise Exception("Not able to parse page for review")

        lock.acquire()
        extract_page_reviews_v2(page_doc, all_reviews)  # Calling function to extract page review for respective pages
        extract_reviewer_profiles_links(page_doc, site_url,
                                        reviewer_profile_links)  # Calling fucntion to extract reviewer profile url for respective page
        lock.release()


# Function to get common details of the drama
# IP : Country : South Korea, here Countryb is title and South Korea is value
# Op : No return, info is stored in the passed dict
# Error : should be handled in parent
def extract_content(title, value, temp_dict):
    if "country" in title:
        temp_dict["country"] = value
    elif "type" in title:
        temp_dict["type_of_show"] = value
    elif "episode" in title:
        temp_dict["episodes"] = int(value)
    elif re.match(r"aired$", title):
        start_dt, end_dt = value.split("-")
        temp_dict["start_date"] = dt.datetime.strptime(start_dt.replace("  ", " ").strip(), "%b %d, %Y")
        temp_dict["end_date"] = dt.datetime.strptime(end_dt.replace("  ", " ").strip(), "%b %d, %Y")
        temp_dict["year"] = temp_dict["start_date"].year
    elif "alsoknown" in title:
        temp_dict["aka_names"] = value.replace("  ", " ")
    elif "screenwriter" in title:
        temp_dict["screenswriter"] = value
    elif "director" in title:
        temp_dict["director"] = value
    elif "genres" in title:
        temp_dict["genres"] = value.replace("  ", " ")
    elif "tag" in title:
        temp_dict["tags"] = re.match(r"(.*)\(vote.*", value, re.I).group(1).strip()
    elif "duration" in title:
        # hr, minu = list(map(int,re.search(r"(\d+)\W*hr\W+(\d+)\W*min",value.replace(" ",""),re.I).groups()))
        hr = 0
        minu = 0
        if "hr" in value:
            hr = int(re.search(r"(\d+)\W*hr", value.replace(" ", ""), re.I).group(1))
        if "min" in value:
            minu = int(re.search(r"(\d+)\W*min", value.replace(" ", ""), re.I).group(1))
            if minu == 60:  # For cases where duration is 60 mins, converting to 1 hr
                minu = 0
                hr += 1

        temp_dict["duration"] = dt.time(hour=hr, minute=minu)
    elif "score" in title:
        rating, no_of_rating = re.search(r"([0-9.]+).*?([0-9,]+)", value, re.I).groups()
        temp_dict["rating"] = float(rating)
        temp_dict["no_of_rating"] = int(no_of_rating.replace(",", ""))
    elif "rank" in title:
        temp_dict["rank"] = int(value.replace("#", ""))
    elif "popular" in title:
        temp_dict["populartity"] = int(value.replace("#", ""))
    elif "contentrating" in title:
        temp_dict["content_rating"] = value


# Function is used to extract a review and convert it into sentences
# IP : html doc and lst to store review sentences
# OP : NO retun, review sentences are stored as a list in the passed list
# Error : Has to be handled at parent
def extract_page_reviews_v2(doc, lst):
    for review in doc.find_all("div", class_=re.compile(r"review-body", re.I)):
        full_text = ""

        for child in review.children:
            if child.name != "div":  # So that text in the small box with reviewer sub ratings is not included
                full_text += child.text

        full_text = re.sub(r"read\s+more", "", re.sub(r"was\s+this\s+review\s+helpful.*$", "", full_text, flags=re.I),
                           flags=re.I)

        lst_of_sentences = []
        for sentence in re.split(r"[.\n\r]", full_text):  # Splitting on . and new line chars to convert to sentences
            if sentence:
                cleaned_sentence = " ".join(
                    re.split(r"\W", re.sub(r"\W+", " ", sentence)))  # only retaining alpha numeric chars
                if cleaned_sentence:
                    lst_of_sentences.append(cleaned_sentence)
        lst.append(lst_of_sentences)


# Function is used to find profile url of the reviewers
# IP : html doc, MDL url to make absolute url path, lst to store info
# OP : no return, info is added in the passed lst
def extract_reviewer_profiles_links(doc, site_url, profile_links):
    for tag in doc.find_all("div", class_=re.compile(r"^review$")):
        person_url = site_url + tag.find("a", href=re.compile(r"profile"), text=True)["href"]
        profile_links.append(person_url)


# Function is used to get location and gender of the reviewers from thier profile urls
# IP : profile url lst, lock while using threads, dict to update ifo
# OP : no return as dict is updated
def extract_drama_reviewer_details_v2(url_set, profile_lock, reviewer_profile_details):
    for url in url_set:
        try:
            person_text = requests.get(url).text
            person_doc = BeautifulSoup(person_text, "html.parser")
        except Exception as e:
            print(f"WARN : Not able to access profile url : {url}, error is : " + str(e))
        else:

            location = ""
            gender = ""

            for li_tag in person_doc.find("ul", class_="list m-b-0").children:
                if "location" in li_tag.text.lower():
                    location = li_tag.text.lower().split(":")[1].strip()
                elif "gender" in li_tag.text.lower():
                    gender = li_tag.text.lower().split(":")[1].strip()
                else:
                    pass

            profile_lock.acquire()
            reviewer_profile_details[url] = {"location": location, "gender": gender}
            profile_lock.release()

        # Function is used to get location and gender of the reviewers from thier profile urls


# IP : profile url lst
# OP : a dict with key as profile url and value is a dict for location and gender
def extract_drama_reviewer_details(url_set):

    reviewer_profile_details = {}  # dict with profile info

    tic = dt.datetime.now()
    print(f"No of profile urls : {len(url_set)}")
    print(f"parsing url : ", end=" ")
    cnt = 1
    for url in url_set:
        print(cnt, end=" ")
        try:
            person_text = requests.get(url).text
            person_doc = BeautifulSoup(person_text, "html.parser")
        except Exception as e:
            print(f"WARN : Not able to access profile url : {url}, error is : " + str(e))
        else:
            reviewer_profile_details[url] = {"location": "", "gender": ""}

            for li_tag in person_doc.find("ul", class_="list m-b-0").children:
                if "location" in li_tag.text.lower():
                    reviewer_profile_details[url]["location"] = li_tag.text.lower().split(":")[1].strip()
                elif "gender" in li_tag.text.lower():
                    reviewer_profile_details[url]["gender"] = li_tag.text.lower().split(":")[1].strip()
                else:
                    pass
        cnt += 1

    toc = dt.datetime.now()
    secs = (toc - tic).seconds

    avg_total_time = secs / len(url_set)

    print(
        f"Time taken to get reviewer details : {avg_total_time // 60} minutes or {avg_total_time // 3600} hrs or {avg_total_time} seconds")

    return reviewer_profile_details


# Function is used to convert nltk tag to wordnet tag
# IP : nltk tag
# OP : wordnet tag
def pos_tagger(nltk_tag):
    if nltk_tag.startswith('J'):
        # print(f"Adj : {wordnet.ADJ}")
        return wordnet.ADJ
    elif nltk_tag.startswith('V'):
        # print(f"VERB : {wordnet.VERB}")
        return wordnet.VERB
    elif nltk_tag.startswith('N'):
        # print(f"NOUN : {wordnet.NOUN}")
        return wordnet.NOUN
    elif nltk_tag.startswith('R'):
        # print(f"ADV : {wordnet.ADV}")
        return wordnet.ADV
    else:
        return None


# Function is used to convert a sentence into a trigram and bigram lists
# IP : sentence and stopword list
# OP : list of bigram and trigram
# Error : Has to be handled by parent
def extract_sentence_trigram_v2(sentence, sw):
    lst_of_words = []
    lemmatizer = WordNetLemmatizer()  # Initializing lemmatizer
    lemmatized_sentence = []

    # to remove stop words from the sentences
    for word in nltk.word_tokenize(sentence.lower()):
        if word in sw:
            continue
        lst_of_words.append(word)

    # derving pos tag to properly lemmatized the words as per pos tags
    pos_tagged = nltk.pos_tag(lst_of_words)
    wordnet_tagged = list(map(lambda x: (x[0], pos_tagger(x[1])), pos_tagged))

    # lemmatization of words
    for word, tag in wordnet_tagged:
        # print(f"{word} : {tag}")
        if tag:  # to remove garbage words not in stopwords
            # Use the tag to lemmatize the token
            x = lemmatizer.lemmatize(word, tag)
            # print(x)
            lemmatized_sentence.append(x)  # lemmatizer.lemmatize(word, tag))
        elif word.isdigit():  # In case the words are integers, retain them
            lemmatized_sentence.append(word)

    if len(lemmatized_sentence) == 2 and lemmatized_sentence[0] != lemmatized_sentence[1]:  # To take care of duplicates
        return [[" ".join(lemmatized_sentence)]]  # Only bigram is returned

    elif len(lemmatized_sentence) > 2:

        bigram_lst = []
        for item in nltk.bigrams(lemmatized_sentence):
            if item:
                if item[0] != item[1]:  # include a bigram only if both words are not same
                    bigram_lst.append(" ".join(item))

        # For 0,1,2 are postions in a trigram, to remove repetition of words following logic is used
        # If 0 == 1 == 2 then remove from list => Handled only in trigram
        # If 0 == 1 and 1 != 2 then bigram 1,2 => handled in bigram
        # If 0 != 1 and 0 == 2 then bigram 0,1 => handles in bigram
        # If 0 != 1 and 1 != 2 only then trigram is made

        trigram_lst = []
        for item in nltk.trigrams(lemmatized_sentence):
            if item[0] != item[1] and item[1] != item[2] and item[0] != item[
                2]:  # Only if all ther are not equal then trigram is made
                trigram_lst.append(" ".join(item))

        return [bigram_lst, trigram_lst]  # a list of both bigram and trigram are returned

    else:
        return []

    # Function is called to get counter of bigrams and trigrams from the list of sentences passed


# IN : list of sentences, bigram counter, trigrams counter and stop words
# OP : no return, info is updated in the passed bigram and trigram counters
# Error : has to be handled by parent
def extract_trigram_v3(review_text_lst, bi_cnter, tri_cnter, sw):
    # review_trigram_cnter = Counter()
    for sentence in review_text_lst:

        gram_lst = extract_sentence_trigram_v2(sentence,
                                               sw)  # Passing sentence to this fucntion to get its bigram and trigram
        # If after lemmatization the sentence has <= 2 words then only bigrams are passed and len of gram_lst is 1
        if len(gram_lst) == 1:
            bi_cnter.update(gram_lst[0])
        elif len(gram_lst) == 2:
            bi_cnter.update(gram_lst[0])
            tri_cnter.update(gram_lst[1])
        else:
            pass


# Function is used to get top 10 people sentiment from the list of bigrams and trigram  provided
# IP : bigram and trigram counter and the number of words we want
# OP : top 10 words used if available
# Error : handled by parent
def processing_cnters(bigram_cnter, trigram_cnter, output_words=10):
    # Logic used
    # For trigrams swapped would be handled by - suffixes
    # Assumed that only one word of the trigram would be swapeed as prefix or suffix
    # eg : grim reaper sunny and sunny grim reaper would be  : grim reaper - sunny
    # If the bigram is not appearing in trigram, then keep the bigram in its order
    # If bigram appearing in trigram then collect all the trigrams with this bigram, then
    # Collect prefixes
    # Collect suffixes
    # Change bigram to => bigram - set(suffixes[s] + prefixe[s]) #Only specify first 5 prefix/suffix for a bigram
    # Delete all the bigrams with prefixes and suffixes and trigrams

    # One main assuption here is that any gram occuring less than 3 times is not provding any useful info
    bigram_cnt = min([len(list(filter(lambda x: x > 2, bigram_cnter.values()))),
                      output_words * 2])  # As there would be reduction, taking a subset of double the words requiered to work with, i.e output_words * 2
    trigram_cnt = min([len(list(filter(lambda x: x > 2, trigram_cnter.values()))),
                       bigram_cnt * 2])  # As trigram are less frequent to coincide, taking range as double than the bigram
    bigram_swap_cnt = min([len(list(filter(lambda x: x > 2, bigram_cnter.values()))),
                           bigram_cnt * 2])  # As there would be reduction, taking a range doubke than requereied , bigram_cnt * 2

    print(f"Bigram cnt considered : {bigram_cnt}")

    # Removing swapped text from the bigrams : eg. sunny reaper and reaper sunny only one with high occurence has to be retained
    bigram_deduped_lst = bigram_cnter.most_common(bigram_swap_cnt)
    for text, cnt in bigram_cnter.most_common(bigram_swap_cnt):
        bigram_text_lst = [i[0] for i in bigram_deduped_lst]
        swapped_text = " ".join(text.split(" ")[::-1])

        if swapped_text in bigram_text_lst:
            index = bigram_text_lst.index(swapped_text)
            # So that the text with higher cnt remains and only the duplicate is removed
            if bigram_deduped_lst[index][1] < cnt:
                bigram_deduped_lst.pop(index)
                bigram_text_lst.pop(index)

    bigram_cnter_copy = bigram_deduped_lst[:bigram_cnt]  # work with only needed
    trigram_cnter_copy = trigram_cnter.most_common(trigram_cnt)  # work with only needed
    # bigram_reject_lst = [] #Remove later
    bigram_final_lst = []
    # trigram_reject_lst = [] #Remove later

    # we will always be evaluating element in the 0 place of bigram_cnter_copy
    while (len(bigram_cnter_copy) != 0):

        bi_text, bi_cnt = bigram_cnter_copy[0]

        tri_text_lst = []

        # making list of trigrams with the bigram
        for trigram_tup in trigram_cnter_copy:
            if bi_text in trigram_tup[0]:
                tri_text_lst.append(trigram_tup)

        # Retaining bigram if bigram is not in trigram , then proceed to evaluate next
        if len(tri_text_lst) == 0:
            bigram_final_lst.append(bi_text)
            bigram_cnter_copy.pop(0)
            continue

        else:

            # Keep bigram, and serach if any suffixes and prefixes are available in the top trigrams

            bigram_cnter_copy.pop(0)  # remove bigram from the main list to change the 0 index element

            prefix_suffix_lst = []

            first_word = bi_text.split(" ")[0]

            # starting search for the suffixes/prefixes in trigrams
            for trigram_tup in tri_text_lst:
                start_index = trigram_tup[0].find(first_word)

                # find the other set of bigram from the trigram : grim reaper sunny : 1. grim reaper (main bigram) 2. reaper sunny (related word)
                related_word = ""
                if start_index == 0:
                    related_bigram = trigram_tup[0][
                                     len(first_word) + 1:]  # grim reaper sunny , grim reaper : reaper sunny
                    related_word = related_bigram.split(" ")[1]
                else:
                    related_bigram = trigram_tup[0][:start_index + len(
                        first_word)]  # sunny grim reaper sunny , grim reaper : sunny grim
                    related_word = related_bigram.split(" ")[0]

                # only add the prefix/suffix if its already not in the list and using a list here to preserve the order of rnk in trigram
                if related_word not in prefix_suffix_lst and len(prefix_suffix_lst) <= 5:
                    prefix_suffix_lst.append(related_word)

                remaning_bigram_values = [i[0] for i in bigram_cnter_copy]

                # delete the related bigram from the main list. As we are going in desc order this would have less occurence and essence is already conveyed in main bigram
                if related_bigram in remaning_bigram_values:
                    related_bigram_index = remaning_bigram_values.index(related_bigram)
                    # bigram_reject_lst.append(bigram_cnter_copy[related_bigram_index])
                    bigram_cnter_copy.pop(related_bigram_index)

                    # removing this trigram as info already conveyed
                # trigram_reject_lst.append(trigram_tup) #Remove later
                trigram_cnter_copy.remove(trigram_tup)

            # join the suffix/preffix to the main bigram in format : main bigram - (semicolon seperated suffix and prefix in rnk ordering)
            if prefix_suffix_lst:
                new_word = bi_text + " - (" + "; ".join(prefix_suffix_lst) + ")"
                bigram_final_lst.append(new_word)  # ((new_word, bi_cnt))

    return bigram_final_lst[:output_words]

#Function is used to categorize a sentence into +ve or -ve sentiment
#IP : index and sentence of the sentence list
#OP : a dict with the sentence label and index
#Error : not eng error is handeled, but rest has to be handled by parent
def categorizing_sentences(info):
    index, sentence = info
    if event.is_set():
        print(f"One of the worker failed, need to exit pid : {os.getpid()}")
        time.sleep(10)
        return

    try:
        #print(f"PID RUNNING : {os.getpid()}") #printing PID to track the process running
        label = sentiment_pipeline(sentence)[0]
        if mapping[label["label"]] == "POSITIVE":
            return({"label":"POSITIVE", "position" : index})
        elif mapping[label["label"]] == "NEGATIVE":
            return({"label":"NEGATIVE", "position" : index})
        else:
            pass
    except Exception as e:
        if detect(sentence) == "en": #in case of error in model if the lang is not eng, ignore, else raise error
            print("failure while using model, error is : " + str(e))
            print(f"Setting event to stop processes in process: {os.getpid()}")
            event.set()
            raise Exception("failure while using model")



# Function is used to parse reviews and gather +ve and -ve sentiment amoung people
# Is also used to gather profile info of the drama reviewers
# IP : review url, dict to store info and set to store reviewer profile info
# OP : No return, info is stored in the dict and set that is passed
# Error : Has to be handled by parent
def get_people_sentiment(url, page_dict, set_reviewer_profile_links):
    # Starting review parsing
    all_reviews = []  # To store all the reviews
    reviewer_profile_links = []  # To store all reviewer profile urls

    review_url = url + "/reviews?xlang=en-US"  # Review url for this drama

    try:
        review_text = requests.get(review_url + "&page=1&sort=helpful").text
        review_doc = BeautifulSoup(review_text, "html.parser")
    except Exception as e:
        print("WARN : Not able to access review url, error is  : " + str(e))
    else:
        # To be executed if review url is accesible
        try:
            max_review_page = int(
                re.search(r"page=(\d+)", review_doc.find("li", class_="page-item last").a["href"]).group(1))
        except Exception as e:
            print("WARN : Review page is only 1, PLEASE CHECK !, error is : " + str(
                e))  # Only a warn as it might actually have only 1 page
            max_review_page = 1

        print(f"Drama has {max_review_page} review pages")

        print("Starting to extract reviews")

        try:
            extract_page_reviews_v2(review_doc,
                                    all_reviews)  # Extracting page review for first page as already available
            extract_reviewer_profiles_links(review_doc, site_url,
                                            reviewer_profile_links)  # Extracting reviewer profile url for first page

            # Before threading
            # for review_page_index in range(2,max_review_page+1):
            #    review_page_url = review_url + f"&page={review_page_index}&sort=helpful"
            #    #print(f"Parsing review page : {review_page_url}")
            #    print(review_page_index, end=" ")
            #    review_page_text = requests.get(review_page_url).text
            #    review_page_doc = BeautifulSoup(review_page_text, "html.parser")
            #    extract_page_reviews_v2(review_page_doc, all_reviews) #Extracting page review for respective pages
            #    extract_reviewer_profiles_links(review_page_doc, site_url, reviewer_profile_links) #Extracting reviewer profile url for respective page

            # After threading
            function_threading(range(2, max_review_page + 1), review_parsing,
                               (review_url, all_reviews, reviewer_profile_links))  # took 3.98 secs

            set_reviewer_profile_links.update(reviewer_profile_links)
            page_dict["reviewer_profile_links"] = set(reviewer_profile_links)

            page_dict["no_of_extracted_reviews"] = len(all_reviews)

            print("\nAll reviews have been extracted")
        except Exception as e:
            print("WARN : Not able to parse review pages properly, error is : " + str(e))
        else:
            # To be executed only is all the reviews are properly extracted

            # Setting stop words
            # Also adding main and support chars names and drama name to it
            extra_omissions = set(
                ["like", "please", "hello", "hi", "everyone", "guys", "people", "many", "much", "more", "always",
                 "even", "though", "however", "got", "get", "made", "make", "korean", "asian", "thai", "chinese", "k",
                 "first", "main"] + re.split(r"\W", page_dict["name"]))
            extra_omissions = extra_omissions | set(
                (page_dict["main_role"].lower() + page_dict["support_role"].lower()).replace(",",
                                                                                             "").split())  # | set(nltk.word_tokenize(page_dict["content"].lower())) #No longer omitting content words

            sw = set(stopwords.words("english")) | extra_omissions

            all_sentences = []
            positive_sentences = []
            negative_sentences = []

            # Converting list of (list of sentences for a review) into a list of all sentences after retaining meaningful sentences
            for list_sentence in all_reviews:
                for sentence in list_sentence:
                    x = " ".join(re.split(r"\W+", sentence)).strip()
                    if x:
                        all_sentences.append(x)
                #all_sentences.extend(list_sentence)

            print("Using model to categorize all labels")
            #all_labels = []
            #delete_indexes = []

            tic = dt.datetime.now()

            try:
                # Using multiprocessing to increase perf of model categorization
                #lst_categories = pool.starmap(categorizing_sentences, list(enumerate(all_sentences)))
                #Optimization
                lst_categories = []
                result = pool.imap(categorizing_sentences, list(enumerate(all_sentences)))
                for i in result:
                    lst_categories.append(i)
            except Exception as e:
                print("ERROR :  failure while using model, error is : " + str(e))
                print("Because of error, closing pool ")
                pool.terminate()
                pool.join()
                print("Pool workers are terminated")
                raise ModelFailureException("Failure while using model")

        
            #Before multiprocessing       #18mins for 6484 sentences
            #for index, sentence in enumerate(all_sentences):
            #    try:
            #        all_labels.extend(sentiment_pipeline(sentence))  # Using model to classify sentiment
            #    except Exception as e:
            #        if detect(sentence) == "en":
            #            print("ERROR : issue while classifying following sentence, error is : " + str(e))
            #            print(sentence)
            #            raise Exception
            #        else:
            #            print(f"There is non english sentence at index : {index}, need to remove this from the list.")
            #            print(sentence)
            #            delete_indexes.append(index)  # Storing indexes to drop as the main list is being iterated

            toc = dt.datetime.now()
            secs = (toc - tic).seconds
            avg_time = secs / len(all_sentences)
            print(
                f"Time taken for categorizing {len(all_sentences)} sentences : {secs // 60} minutes or {secs // 3600} hrs or {secs} seconds")
            print(
                f"Average Time taken for categorizing  : {avg_time // 60} minutes or {avg_time // 3600} hrs or {avg_time} seconds")

            print("categorization is complete")

            for category in lst_categories:
                if not category:
                    pass
                elif category["label"] == "POSITIVE":
                    positive_sentences.append(all_sentences[category["position"]])
                else:
                    negative_sentences.append(all_sentences[category["position"]])

            # removing sentences that are not in english
            #if delete_indexes:
            #    for index in delete_indexes:
            #        all_sentences.pop(index)

            page_dict["Total_sentences"] = len(all_sentences)

            # Categorizing +ve and -ve sentences into diff lists
            #for sentence, label in zip(all_sentences, all_labels):
            #    if mapping[label["label"]] == "POSITIVE":
            #        positive_sentences.append(sentence)
            #    elif mapping[label["label"]] == "NEGATIVE":
            #        negative_sentences.append(sentence)
            #    else:
            #        pass

            print("Starting with bigram and trigram extractions from reviews")
            # extracting trigram and bigram from drama content to remove them before raking
            content_bigram = Counter()
            content_trigram = Counter()

            extract_trigram_v3(re.split(r"[.\n\r]", page_dict["content"]), content_bigram, content_trigram, sw)

            # Making thier counts 999 ~ infy to remove thier counts from counter easily
            for key in content_bigram:
                content_bigram[key] = 999

            for key in content_trigram:
                content_trigram[key] = 999

            # iterating through +ve and -ve sentences to extract peoples sentiment
            for label, lst_sentence in zip(["POSITIVE", "NEGATIVE"], [positive_sentences, negative_sentences]):
                bigram_counter = Counter()
                trigram_counter = Counter()
                extract_trigram_v3(lst_sentence, bigram_counter, trigram_counter,
                                   sw)  # Calling function to update bigram and trigram counters with text

                # Removing content bigrams and trigrams from the list
                bigram_counter = bigram_counter - content_bigram
                trigram_counter = trigram_counter - content_trigram

                # Calling function which ranks the top 10 word assosiations
                label_sentiment = processing_cnters(bigram_counter, trigram_counter)
                page_dict[label + "_people_sentiment"] = ", ".join(label_sentiment)
                page_dict[label + "_sentences"] = len(lst_sentence)  # Also adding total number of words


#Keeping the model and its mapping in outside the if condition to be accessable by concurrent processes
#sentiment_pipeline = pipeline(model="cardiffnlp/twitter-roberta-base-sentiment")
#mapping = {"LABEL_0": "NEGATIVE", "LABEL_1": "NEUTRAL", "LABEL_2": "POSITIVE"}

def init_process_func(shared_event):
    global event
    global sentiment_pipeline
    global mapping
    event = shared_event
    sentiment_pipeline = pipeline(model="cardiffnlp/twitter-roberta-base-sentiment")
    try:
        sentiment_pipeline("Model is working!!")
    except Exception as e:
        print("ERROR : model is not working, error is : " + str(e))
        raise Exception("Unable to initialize model")
    else:
        print(f"For {os.getpid()} : Model is working")
    mapping = {"LABEL_0": "NEGATIVE", "LABEL_1": "NEUTRAL", "LABEL_2": "POSITIVE"}


if __name__ == "__main__":

    # Initializing model to be used, its a twitter base roberta model used from huggingface.com
    #print("Initializing the model to use")
    #try:
    #    sentiment_pipeline("Model is working!!")
    #except Exception as e:
    #    print("ERROR : model is not working, error is : " + str(e))
    #    sys.exit(1)
    #else:
    #    print("Model is working")

    #pool = Pool(4)

    # Setting variables for parsing MyDramaList site
    site_url = "https://mydramalist.com"  # MDL URL, useful for browsing pages
    main_url = "https://mydramalist.com/search?adv=titles&ty=68,83,86&st=3&so=popular"

    print(f"Main page URL : {main_url}")

    print("Parsing Main Page!")
    try:
        main_page = requests.get(main_url).text
        doc = BeautifulSoup(main_page, "html.parser")
    except Exception as e:
        print("ERROR : Not able to access mdl main page, error is : " + str(e))
        sys.exit(1)

    # Getting max no of pages to parse
    # Getting the max number of pages from the scroll buttons at the page bottm
    try:
        max_page = int(re.search(r"(?<=page=)\d+", doc.find("li", class_="page-item last").a["href"]).group())
    except Exception as e:
        print("WARN : Main page has only 1 page, PLEASE CHECK!!, error is : " + str(e))
        max_page = 1  #

    print(f"Main page has {max_page} pages")

    link_mapping = {}  # this variable will have all the drama and its url
    print("Gathering url of all the drama in all pages")
    get_link_mapping(doc,
                     link_mapping)  # Calling function to get all drama urls for first page as we already have the doc

    ### Before threading
    # print(f"Page Parsing :")
    # for page_index in range(2,max_page+1):
    #    page_url = main_url + f"&page={page_index}"
    #    page_text = requests.get(page_url).text
    #    page_doc = BeautifulSoup(page_text, "html.parser")
    #    print(page_index, end=" ")
    #    get_link_mapping(page_doc, link_mapping)

    # Iterating through pages and getting all the drama urls and making it faster through threading
    try:
        function_threading(range(2, max_page + 1), link_parsing, (link_mapping,))
    except Exception as e:
        print("Error while parsing url using threading, error is : " + str(e))
        sys.exit(1)

    print("\nFinished gathering drama urls")

    # Script to get contents for each drama
    detailed_dict = {}  # Final dict with all the drama and thier contents
    set_reviewer_profile_links = set()  # Final set used to gather reviewer profile details

    print("Starting to gather info on each drama")

    try:
        print("Starting process pool for multiprocessing")
        shared_event = Manager().Event()
        pool = Pool(4, initializer=init_process_func, initargs=(shared_event,))

        print("PID of workers that will be used : ")
        print("###############")
        for worker in pool._pool:
            print(worker.pid)
        print("###############")
        print("Initializing processes")
        pool.map(time.sleep, 2)

        # Iterating through each drama url page
        for drama, url in list(link_mapping.items())[:1]:

            drama_tic = dt.datetime.now()

            print(f"Parsing {drama} : {url}")
            page_dict = {}  # Temp dict which maintains info only about single drama

            try:
                drama_text = requests.get(url).text
                drama_doc = BeautifulSoup(drama_text, "html.parser")
            except Exception as e:
                print("ERROR : Not able to access url for this drama, error is : " + str(e))
                sys.exit(1)

            page_dict["name"] = drama
            page_dict["content"] = drama_doc.find("div", class_="show-synopsis").find("span").text

            try:
                page_dict["no_of_reviews"] = int(
                    drama_doc.find("a", class_="text-primary", text=re.compile(r"\d+\s+user", re.I)).text.split()[0])
                page_dict["no_of_viewers"] = int(
                    drama_doc.find(text=re.compile(r"#\s+of\s+watcher", re.I)).parent.b.text.replace(",", "").strip())
            except Exception as e:
                print("WARN : Not able to parse #review or #view, error is : " + str(e))

            # Scrip to get platform to watch
            print("Scrip to get platform to watch")
            platform = []
            try:
                for row_tag in drama_doc.find("div", class_="box-body wts").div.children:
                    if re.match(r" +$", row_tag.text):  # Check to filter out empty lines
                        continue
                    platform.append(row_tag.text.strip())
            except Exception as e:
                print("WARN : Not able to parse platforms to watch, error is : " + str(e))

            page_dict["where_to_watch"] = ", ".join(platform)

            # Scrip to get main role and support role details
            print("Scrip to get main role and support role details")

            cast_url = site_url + drama_doc.find("a", href=re.compile(r"/cast", re.I), text=re.compile(r"view", re.I))["href"]

            try:
                cast_text = requests.get(cast_url).text
                cast_doc = BeautifulSoup(cast_text, "html.parser")
            except Exception as e:
                print("WARN : Not able to access cast url for drama, error is : " + str(e))
            else:
                # Only to be execute if cast_doc is accesible
                main_role = []
                try:
                    for row_tag in cast_doc.find("h3",
                                                 text=re.compile(r"main\s+role", re.I)).next_sibling.next_sibling.children:
                        if re.match(r" +$", row_tag.text):  # Check to filter out empty lines
                            continue
                        main_role.append(row_tag.find("a", class_="text-primary").text.strip())
                except Exception as e:
                    print("WARN : Not able to parse main role!!, error is : " + str(e))

                page_dict["main_role"] = ", ".join(main_role)

                support_role = []
                try:
                    for row_tag in cast_doc.find("h3",
                                                 text=re.compile(r"support\s+role", re.I)).next_sibling.next_sibling.children:
                        if re.match(r" +$", row_tag.text):
                            continue
                        support_role.append(row_tag.find("a", class_="text-primary").text.strip())
                except Exception as e:
                    print("WARN : Not able to parse Support Roles!!, error is : " + str(e))

                page_dict["support_role"] = ", ".join(support_role)

            # Script to get other details of drama
            print("Script to get other details of drama")
            try:
                for upper_child_tag in drama_doc.find("div", class_="show-detailsxss").children:
                    if re.match(r" +$", upper_child_tag.text):  # Check to filter out empty lines
                        continue

                    for child in upper_child_tag.children:
                        if re.match(r" +$", child.text):  # Check to filter out empty lines
                            continue

                        try:
                            if ":" not in child.text:
                                continue
                            title, value = child.text.split(":", 1)
                            title = title.lower().strip().replace(" ", "")
                            value = value.strip()
                            extract_content(title, value, page_dict)
                        except Exception as e:
                            print(f"WARN : Not able to parse following content : {child.text}, error is : " + str(e))
            except Exception as e:
                print("WARN : Not able to gather other content details for this drama, error is : " + str(e))

            # Scrip to get people sentiment from reviews
            print("Scrip to get people sentiment from reviews")
            try:
                get_people_sentiment(url, page_dict,
                                     set_reviewer_profile_links)  # Calling function to get +ve and -ve sentiments and #
            except ModelFailureException as m:
                print("ERROR : Issue while using model, need to quit. Error is : " + str(m))
                sys.exit(1) #Need to stop the program else will go on even with the model issues
            except Exception as e:
                print("WARN : Not able to get sentiment data for this drama, error is : " + str(e))

            detailed_dict.update({drama: page_dict})

            drama_toc = dt.datetime.now()
            drama_secs = (drama_toc - drama_tic).seconds
            print(f"Drama Time taken : {drama_secs // 60} minutes or {drama_secs // 3600} hrs seconds")

            print(detailed_dict.keys())
    except Exception as e:
        print("Error while gathering drama details, error is : " + str(e))
        sys.exit(1)
    finally:
        pool.close()
        pool.join()

    # Before threading
    # reviewer_profile_details = extract_drama_reviewer_details(set_reviewer_profile_links) #calling fuction to get location and gender info on each profile url


    # Code for updating reviwer details in the detailed_dict using threading
    reviewer_profile_details = {}  # dict with profile url details

    tic = dt.datetime.now()

    try:
        function_threading(list(set_reviewer_profile_links), extract_drama_reviewer_details_v2,
                           (reviewer_profile_details,))  # using threading to optimize the scrapping
    except Exception as e:
        print("ERROR : Not able to parse profile urls while threading, error is : " + str(e))
    

    toc = dt.datetime.now()
    secs = (toc - tic).seconds

    avg_total_time = secs / len(set_reviewer_profile_links)

    print(
        f"Time taken to get reviewer details : {avg_total_time // 60} minutes or {avg_total_time // 3600} hrs or {avg_total_time} seconds")

    reviewer_tic = dt.datetime.now()
    for drama in detailed_dict:
        # initalizing counters for each drama
        reviewer_location_cnter = Counter()
        reviewer_gender_cnter = Counter()

        # as per the reviewers in the drama, location and gender of those are added in the counter for this drama
        for profile_url in detailed_dict[drama]["reviewer_profile_links"]:
            if reviewer_profile_details.get(profile_url):
                reviewer_location_cnter.update([reviewer_profile_details[profile_url]["location"]])
                reviewer_gender_cnter.update([reviewer_profile_details[profile_url]["gender"]])

        detailed_dict[drama].update(
            {"reviewer_location_info": reviewer_location_cnter, "reviwer_gender_info": reviewer_gender_cnter})
        # del detailed_dict[drama]["reviewer_profile_links"] #Use later

    reviewer_toc = dt.datetime.now()
    reviewer_secs = (reviewer_toc - reviewer_tic).seconds
    avg_time = reviewer_secs / len(detailed_dict)

    print(f"reviewer detail update Time taken : {avg_time // 60} minutes or {avg_time // 3600} hrs or {avg_time} seconds")

    df = pd.DataFrame(detailed_dict.values())

    df.to_csv(r"C:\Users\Anitta\Desktop\Jobs\Portfolio\MDL\df.csv", encoding='utf-8')