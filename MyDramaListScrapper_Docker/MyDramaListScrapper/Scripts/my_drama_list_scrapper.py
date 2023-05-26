# To get processing time of each cell
# !pip install -q transformers
# !pip install transformers[tf-cpu]
# !pip install ipython-autotime
# !pip install langdetect
# Import required python libraries
from multiprocessing import Pool, Manager
import datetime as dt
import pandas as pd
import sys
import time
import os
from transformers import pipeline
from langdetect import detect
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'
#Import custom packages
sys.path.append('../')
from SharedObjects.shared_objects import *
from WebScrapper.scrapping_web import extract_url_details, get_link_mapping, link_parsing, extract_cast_details, function_threading
from ContentExtractor.content_extractor import extract_general_details, extract_platforms_to_watch
from ReviewProcessor.review_scrapper import extract_drama_reviewer_details_v2, reviewer_profile_updates
from ReviewProcessor.review_processor import extract_reviews_to_sentences, create_grams

#### Error handling Info ###
#As the process takes a lot of time to run, to preserve the least info that is gathered, most of the errors will only populate blank in the df.
#the main errors where the script will fail are :
#If not able to access main url and its respective page continuations
#If not able to access drama urls
#If there is any issue with the model initialization or the step after that i.e creating grams
#If there is any major error while gathering drama details


def categorizing_sentences(info):
    """
    Function is used to categorize a sentence into +ve, -ve or neutral sentiment
    IP : index and sentence of the sentence list
    OP : a dict with the sentence label and index
    Error : not eng error is handled, but rest has to be handled by parent
    """
    index, sentence = info
    if event.is_set():
        print(f"One of the worker failed, need to exit pid : {os.getpid()}")
        time.sleep(1)
        return

    try:
        label = sentiment_pipeline(sentence)[0]
        if mapping[label["label"]] == "POSITIVE":
            return ({"label": "POSITIVE", "position": index})
        elif mapping[label["label"]] == "NEGATIVE":
            return ({"label": "NEGATIVE", "position": index})
        else:
            pass
    except Exception as e:
        if detect(sentence) == "en":  # in case of error in model if the lang is not eng, ignore, else raise error
            print(f"Error while using model, setting event to stop processes in process: {os.getpid()}")
            event.set()
            raise Exception(f"Failure while using model for sentence : \n##############\n{sentence}\n############ \nerror is : " + str(e))


def write_to_file(x):
    """
    Function to write data into csv
    IP : data to write
    OP : No return, just populates csv
    ERROR : has to be handled by parent
    """
    #Creating dataframe with these info
    df = pd.DataFrame(x)
    file_path = r"/home/Output_files/Top_5000_popular_drama_details_from_mydramalist.csv"
    df.to_csv(file_path, encoding='utf-8')
    print(f"Data has been written in file : {file_path}")


def init_process_func(shared_event):
    """
    Function is an initialization function to set global variables in each process spawned while using multiprocessing using pools
    IP: manager event is passed
    OP: No op as only setting is done here
    Error : Has to be handled by parent
    """
    global event
    global sentiment_pipeline
    global mapping
    event = shared_event
    event.clear() #Clearing the event before reinitializing again
    print(f"For {os.getpid()} : Event status is {event.is_set()}")

    sentiment_pipeline = pipeline(model="cardiffnlp/twitter-roberta-base-sentiment")
    try:
        sentiment_pipeline("Model is working!!")
    except Exception as e:
        print("ERROR : model is not working, error is : " + str(e))
        raise Exception("Unable to initialize model")
    else:
        print(f"For PID {os.getpid()} : Model is working")
    mapping = {"LABEL_0": "NEGATIVE", "LABEL_1": "NEUTRAL", "LABEL_2": "POSITIVE"}


#Changes for 5000
#   changed name of file to write to
#   commented out max_page=25 in the main code block to run for all the 250 pages
#   removing [:1] from iteration to iterate all dramas
#   added flush to print statements so that child processes can write to the logs without waiting for termination
if __name__ == "__main__":
    print(f"Main page URL : {main_url}")

    print("Parsing Main Page!")
    doc, max_page = extract_url_details(main_url, check_max_page=True) #returns html and max pages in a url
    #max_page = 25 #Change back
    if not doc:
        print("ERROR : Not able to access mdl main page!!")
        sys.exit(1)
    elif max_page == 1:
        print(f"WARN : Main page has only 1 page, PLEASE CHECK!! {main_url}")
    else:
        print(f"Main page has {max_page} pages")

    link_mapping = {}  # this variable will have all the drama and its url
    print("Gathering url of all the drama in all pages")
    try:
        get_link_mapping(doc,
                     link_mapping)  # Calling function to get all drama urls for first page as we already have the doc
    except Exception as e:
        print("ERROR : Failure while getting drama links, error is : " + str(e))
        sys.exit(1)

    # Iterating through pages and getting all the drama urls and making it faster through threading
    if max_page > 1:
        try:
            function_threading(range(2, max_page + 1), link_parsing, (link_mapping,))
        except Exception as e:
            print("ERROR : failure while parsing url using threading, error is : " + str(e))
            sys.exit(1)

    print("\nFinished gathering drama urls")
    print(f"There are {len(link_mapping)} dramas to parse!!")

    # Script to get contents for each drama
    detailed_dict = {}  # Final dict with all the drama and their contents
    set_reviewer_profile_links = set()  # Final set used to gather reviewer profile details

    print("Starting to gather info on each drama")

    try:
        print("Starting process pool for multiprocessing")
        shared_event = Manager().Event()
        no_of_processors = 10
        pool = Pool(no_of_processors, initializer=init_process_func, initargs=(shared_event,))
        print("PID of workers that will be used : ")
        print("###############")
        for worker in pool._pool:
            print(worker.pid)
        print("###############")
        print("Initializing processes", flush=True)
        pool.map(time.sleep, [10])

        # Iterating through each drama url page
        cnt = 1
        for drama, url in list(link_mapping.items()):

            drama_tic = dt.datetime.now()

            print(f"Parsing {drama} : {url}", flush=True)
            page_dict = {}  # Temp dict which maintains info only about single drama

            page_dict["name"] = drama
            page_dict["content"] = "" #As in case of any error, this variable needs to be present for the create_grams function to work

            drama_doc = extract_url_details(url)
            if not drama_doc:
                raise Exception(f"Not able to access url for this drama, exiting process")

            try:
                extract_general_details(drama_doc, page_dict) #Updates page_dict with details like : content, country, etc.
            except Exception as e:
                print("WARN : Not able to parse general drama details, error is : " + str(e))

            # Scrip to get platform to watch
            print("Scrip to get platform to watch")
            try:
                extract_platforms_to_watch(drama_doc, page_dict) #Updates page_dict with platforms to watch like : Netflix, Apple Pay, etc.
            except Exception as e:
                print("WARN : Not able to parse platforms to watch, error is : " + str(e))

            # Scrip to get main role and support role details
            print("Scrip to get main role and support role details")

            try:
                extract_cast_details(url, page_dict) #Updates page_dict with main and support cast details
            except Exception as e:
                print("WARN : Not able to extract cast details, error is : " + str(e))

            # Scrip to get people sentiment from reviews
            print("Scrip to get people sentiment from reviews")

            try:
                # Extracting reviews into sentences
                all_sentences = []
                all_sentences = extract_reviews_to_sentences(url, page_dict, set_reviewer_profile_links) #returns all review sentences
                if len(all_sentences) == 0:
                    raise Exception("No reviews found")
            except Exception as e:
                if str(e) != "No reviews found": #As there is no review directly jump to after else block of try catch
                    print("WARN : Not able to get people sentiments , error is : " + str(e))
                else:
                    print("WARN : No reviews found")
                    #In case of error, don't fail the process, just continue to gather the next details
            else:
                # To be executed only is all the reviews are properly extracted
                print("Using model to categorize all labels", flush=True)  
                run_flag = 0
                while True: #Looping twice in case of any error, to check if the process failed due to intermittent issues
                    tic = dt.datetime.now()
                    try:
                        # Using multiprocessing to increase perf of model categorization
                        lst_categories = []
                        cnk_size = 1
                        result = pool.imap_unordered(categorizing_sentences, list(enumerate(all_sentences)),
                                                     chunksize=cnk_size)
                        for i in result:
                            lst_categories.append(i)

                    except Exception as e:
                        print("Failure while using model, closing pool ")
                        pool.terminate()
                        pool.join()
                        print("Pool workers are terminated")
                        if run_flag == 0:
                            print("WARN : Model failed with error : " + str(e))
                            print("Starting pool again to try one more time ...", flush=True)
                            #Setting run flag to 1 for another run, to check if the issue is intermittent
                            run_flag = 1
                            #Re-initializing pool to start once more
                            pool = Pool(no_of_processors, initializer=init_process_func, initargs=(shared_event,))
                            print("PID of workers that will be used : ")
                            print("###############")
                            for worker in pool._pool:
                                print(worker.pid)
                            print("###############")
                            print("Initializing processes")
                            pool.map(time.sleep, [10])
                        elif run_flag == 1:
                            #End the process
                            raise Exception("Failure while using model, error is : " + str(e))
                    else:
                        #Only execute once if successful execution
                        print("categorization is complete")
                        break
                    finally:
                        toc = dt.datetime.now()
                        secs = (toc - tic).seconds
                        avg_time = secs / len(all_sentences)
                        print(
                            f"Time taken for categorizing {len(all_sentences)} sentences : {secs // 60} minutes or {secs // 3600} hrs or {secs} seconds")
                        print(
                            f"Average Time taken for categorizing  : {avg_time // 60} minutes or {avg_time // 3600} hrs or {avg_time} seconds")

                #Calling scrip to get top 10 +ve and -ve words used to describe this drama if available
                try:
                    create_grams(all_sentences, lst_categories, page_dict) #Updates page_dict
                except Exception as e:
                    raise Exception(f"Not able to create top 10 words, error is : " + str(e))

            detailed_dict.update({drama: page_dict})

            drama_toc = dt.datetime.now()
            drama_secs = (drama_toc - drama_tic).seconds
            print(f"Drama Time taken : {drama_secs // 60} minutes or {drama_secs // 3600} hrs or {drama_secs} seconds")

            print(detailed_dict.keys())
            print(f"Processing completed for {cnt} dramas", flush=True)
            cnt += 1
    except Exception as e:
        print("ERROR : while gathering drama details, error is : " + str(e))
        detailed_dict.update({drama: page_dict})
        write_to_file(detailed_dict.values())
        sys.exit(1)
    finally:
        print("Closing pool")
        pool.close()
        pool.join()

    # Code for updating reviewer details in the detailed_dict using threading
    reviewer_profile_details = {}  # dict with profile url details

    tic = dt.datetime.now()

    if len(set_reviewer_profile_links) != 0:
        try:
            function_threading(list(set_reviewer_profile_links), extract_drama_reviewer_details_v2,
                               (reviewer_profile_details,))  # using threading to optimize the scrapping
        except Exception as e:
            print("ERROR : Not able to parse profile urls while threading, error is : " + str(e))
    else:
        print("WARN : There is no reviewer info to extract")


    toc = dt.datetime.now()
    secs = (toc - tic).seconds

    print(
        f"Time taken to get reviewer details : {secs // 60} minutes or {secs // 3600} hrs or {secs} seconds")

    reviewer_tic = dt.datetime.now()

    try:
        reviewer_profile_updates(detailed_dict, reviewer_profile_details)
    except Exception as e:
        print("WARN : Failure while updating df with reviewer profile info, error is : " + str(e))
        #No exit as we want to have atleast available data written into csv

    reviewer_toc = dt.datetime.now()
    reviewer_secs = (reviewer_toc - reviewer_tic).seconds

    print(f"reviewer detail update Time taken : {reviewer_secs // 60} minutes or {reviewer_secs // 3600} hrs or {reviewer_secs} seconds")

    try:
        write_to_file(detailed_dict.values())
    except Exception as e:
        print("ERROR : Failure while writing to file, error is : " + str(e))
        sys.exit(1)


