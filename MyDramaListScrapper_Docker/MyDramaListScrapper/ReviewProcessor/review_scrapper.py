# Import python packages
import re
from collections import Counter
# Import custom packages
from SharedObjects.shared_objects import *
from WebScrapper.scrapping_web import extract_url_details


def review_parsing(review_page_range, lock, review_url, all_reviews, reviewer_profile_links):
    """
    Function called to iterating through pages passed and getting all the reviews
    IP: index number of pages to parse, general url, lst to update with reviews, set to update with reviewer
    OP: No return, info is updated in the passed lst and set
    ERROR : has to be handled by parent
    """
    for page_index in review_page_range:
        page_url = review_url + f"&page={page_index}&sort=helpful"

        page_doc = extract_url_details(page_url)
        if not page_doc:
            print(f"ERROR : Not able to parse page no : {page_index} of the reviews page")
            raise Exception("Not able to parse page for review")

        lock.acquire()
        extract_page_reviews_v2(page_doc, all_reviews)  # Calling function to extract page review for respective pages
        extract_reviewer_profiles_links(page_doc, site_url,
                                        reviewer_profile_links)  # Calling function to extract reviewer profile url for respective page
        lock.release()


def extract_page_reviews_v2(doc, lst):
    """
    Function is used to extract a review and convert it into sentences
    IP : html doc and lst to store review sentences
    OP : NO return, review sentences are stored as a list in the passed list
    Error : Has to be handled at parent
    """
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
                    re.split(r"\W", re.sub(r"\W+", " ", sentence)))  # only retaining alphanumeric chars
                if cleaned_sentence:
                    lst_of_sentences.append(cleaned_sentence)
        lst.append(lst_of_sentences)


def extract_reviewer_profiles_links(doc, site_url, profile_links):
    """
    Function is used to find profile url of the reviewers
    IP : html doc, MDL url to make absolute url path, lst to store info
    OP : no return, info is added in the passed lst
    """
    for tag in doc.find_all("div", class_=re.compile(r"^review$")):
        person_url = site_url + tag.find("a", href=re.compile(r"profile"), text=True)["href"]
        profile_links.append(person_url)


def extract_drama_reviewer_details_v2(url_set, profile_lock, reviewer_profile_details):
    """
    Function is used to get location and gender of the reviewers from their profile urls
    IP : profile url lst, lock while using threads, dict to update ifo
    OP : no return as dict is updated
    """
    for url in url_set:
        person_doc = extract_url_details(url)
        if not person_doc:
            print(f"WARN : Not able to access profile url : {url}")
            continue

        location = ""
        gender = ""

        try:
            for li_tag in person_doc.find("ul", class_="list m-b-0").children:
                if "location" in li_tag.text.lower():
                    location = li_tag.text.lower().split(":")[1].strip()
                elif "gender" in li_tag.text.lower():
                    gender = li_tag.text.lower().split(":")[1].strip()
                else:
                    pass
        except Exception as e:
            print(f"WARN : Not able to get details for profile : {url}")

        profile_lock.acquire()
        reviewer_profile_details[url] = {"location": location, "gender": gender}
        profile_lock.release()


def reviewer_profile_updates(detailed_dict, reviewer_profile_details):
    """
    Function is used to extract and update a summary of location and gender info of reviewers for respective dramas
    IP : dict to update, dict with reviewer location and gender details
    OP : No return as dict passed is updated
    ERROR : has to be handled in parent
    """
    for drama in detailed_dict:
        # initializing counters for each drama
        reviewer_location_cnter = Counter()
        reviewer_gender_cnter = Counter()

        # as per the reviewers in the drama, location and gender of those are added in the counter for this drama
        for profile_url in detailed_dict[drama].get("reviewer_profile_links", {}):
            if reviewer_profile_details.get(profile_url):
                reviewer_location_cnter.update([reviewer_profile_details[profile_url]["location"]])
                reviewer_gender_cnter.update([reviewer_profile_details[profile_url]["gender"]])

        detailed_dict[drama].update(
            {"reviewer_location_info": reviewer_location_cnter, "reviewer_gender_info": reviewer_gender_cnter})
        del detailed_dict[drama]["reviewer_profile_links"] #to remove the profile details from the dataset