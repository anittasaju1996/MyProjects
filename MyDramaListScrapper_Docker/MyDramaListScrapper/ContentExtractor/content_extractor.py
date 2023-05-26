# Import python packages
import re
import datetime as dt


def extract_general_details(drama_doc, page_dict):
    """
    Function is used to extract all general details in the html
    IP : html, dict to updates details
    OP : No return as everything is updated in the passed dict
    ERROR : has to be handled by parent
    """
    page_dict["content"] = drama_doc.find("div", class_="show-synopsis").find("span").text
    page_dict["no_of_reviews"] = int(
        drama_doc.find("a", class_="text-primary", text=re.compile(r"\d+\s+user", re.I)).text.split()[0])
    page_dict["no_of_viewers"] = int(
        drama_doc.find(text=re.compile(r"#\s+of\s+watcher", re.I)).parent.b.text.replace(",", "").strip())

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
                #No error raised as in case any error, can continue to get to info of other details


def extract_platforms_to_watch(drama_doc, page_dict):
    """
    Function is used to extract platforms to watch the drama
    IP : hrml, dict to fed info
    OP : No return as dict passed is set
    ERROR : has to be handled at parent
    """
    platform = []
    if drama_doc.find_all("h3", text=re.compile(r"where\s+to\s+watch", re.I)):
        for row_tag in drama_doc.find("div", class_="box-body wts").div.children:
            if re.match(r" +$", row_tag.text):  # Check to filter out empty lines
                continue
            platform.append(row_tag.text.strip())
    page_dict["where_to_watch"] = ", ".join(platform)


def extract_content(title, value, temp_dict):
    """
    Function to get common details of the drama
    IP : Country : South Korea, here Country is title and South Korea is value
    Op : No return, info is stored in the passed dict
    Error : should be handled in parent
    """
    if "country" in title:
        temp_dict["country"] = value
    elif "type" in title:
        temp_dict["type_of_show"] = value
    elif "episode" in title:
        temp_dict["episodes"] = int(value)
    elif re.match(r"aired$", title):
        #start_dt, end_dt = value.split("-")
        #temp_dict["start_date"] = dt.datetime.strptime(start_dt.replace("  ", " ").strip(), "%b %d, %Y")
        #temp_dict["end_date"] = dt.datetime.strptime(end_dt.replace("  ", " ").strip(), "%b %d, %Y")
        #temp_dict["year"] = temp_dict["start_date"].year
        if "-" in value:
            start_dt, end_dt = value.split("-")
            temp_dict["end_date"] = dt.datetime.strptime(end_dt.replace("  "," ").strip(), "%b %d, %Y")
        else:
            start_dt = value

        temp_dict["start_date"] = dt.datetime.strptime(start_dt.replace("  "," ").strip(), "%b %d, %Y")
        temp_dict["year"] = temp_dict["start_date"].year
    elif "alsoknown" in title:
        temp_dict["aka_names"] = value.replace("  ", " ")
    elif "screenwriter" in title:
        temp_dict["screenwriter"] = value
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
        temp_dict["popularity"] = int(value.replace("#", ""))
    elif "contentrating" in title:
        temp_dict["content_rating"] = value


