# Import python packages
from bs4 import BeautifulSoup
import requests
import re
import threading
import math
#Import custom packages
from SharedObjects.shared_objects import *


def extract_url_details(url, check_max_page=False):
    """
    Function is used to extract the html and max page in a url
    IP : url to parse, a boolean ind to check if max pages are required
    OP : html[, max page if ind id True]
    ERROR : Is handled in the module, doc and max_page are returned as '' and 0 if there is any error
    """
    url_doc = None
    max_page = 0
    try:
        url_text = requests.get(url).text
        url_doc = BeautifulSoup(url_text, "html.parser")
    except Exception as e:
        print(f"WARN : Not able to parse {url}, error is : " + str(e))
        if check_max_page:
            return url_doc, max_page
        else:
            return url_doc

    if check_max_page:
        # Getting max no of pages to parse
        # Getting the max number of pages from the scroll buttons at the page bottom
        if url_doc.find("li", class_="page-item last"):
            max_page = int(
                re.search(r"page=(\d+)", url_doc.find("li", class_="page-item last").a["href"]).group(1))
        elif url_doc.find("li", class_="page-item next"):
            max_page = int(re.search(r"page=(\d+)",
                                            url_doc.find("li", class_="page-item next").previous_sibling.a[
                                                "href"]).group(1))
        else:
            max_page = 1

        return url_doc, max_page

    else:
        return url_doc


def function_threading(lst, f, arguments):
    """
    Function is called to increase function performance through threading
    It splits the passed list into parts and runs different threads with the same function and arguments but different parts of list
    IP : Full lst to be processed, function name, extra arguments to be passed
    Exists after all the threads are complete
    Error  : handled in parent
    """
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


def get_link_mapping(doc, link_dict):
    """
    Function to fetch name and link from the page
    Input : Beautiful Soup html , a dictionary to update the name and link found
    Output : No return, info is stored in the passed dict
    Error : print msg but continue with rest
    """
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
            raise Exception(f"{i.a.text.lower()} has no link!, error is : " + str(e))  # o/p to console in case of any errors


def link_parsing(page_range, lock, link_mapping):
    """
    Function called to iterating through pages passed and getting all the drama urls
    """
    for page_index in page_range:
        page_url = main_url + f"&page={page_index}"
        page_doc = extract_url_details(page_url)
        if not page_doc:
            raise Exception(f"Not able to parse page no : {page_index} of the main page")

        lock.acquire()
        get_link_mapping(page_doc, link_mapping)  # Calling function to get all drama urls for respective page
        lock.release()


def extract_cast_details(drama_url, page_dict):
    """
    Function is used to extract main and support cast details
    IP : url of drama, dict to update
    OP : No return as dict passed is updated
    ERROR : has to be handled in parent
    """
    #cast_url = site_url + drama_doc.find("a", href=re.compile(r"/cast", re.I), text=re.compile(r"view", re.I))["href"]
    cast_url = drama_url + "/cast"

    cast_doc = extract_url_details(cast_url)
    if not cast_doc:
        raise Exception(f"Not able to access cast url for drama {cast_url}")

    # Only to be execute if cast_doc is accessible
    main_role = []
    try:
        for row_tag in cast_doc.find("h3",
                                     text=re.compile(r"main\s+role", re.I)).next_sibling.next_sibling.children:
            if re.match(r" +$", row_tag.text):  # Check to filter out empty lines
                continue
            main_role.append(row_tag.find("a", class_="text-primary").text.strip())
    except Exception as e:
        print(f"WARN : Not able to parse main role!! {cast_url}, error is : " + str(e))
    finally:
        page_dict["main_role"] = ", ".join(main_role)

    support_role = []
    try:
        for row_tag in cast_doc.find("h3",
                                     text=re.compile(r"support\s+role", re.I)).next_sibling.next_sibling.children:
            if re.match(r" +$", row_tag.text):
                continue
            support_role.append(row_tag.find("a", class_="text-primary").text.strip())
    except Exception as e:
        print(f"WARN : Not able to parse Support Roles!! {cast_url}, error is : " + str(e))
    finally:
        page_dict["support_role"] = ", ".join(support_role)

