## About this project
This project is used to scrap information of all the dramas in [MyDramaList](https://mydramalist.com/) website . 

The dataset generated is a csv that consists of all completed (Drama + Drama Special) from MyDramaList and covers drama across multiple countries like South Korea, China, Japan, Thailand, Taiwan, etc. It gives a comprehensive report of all general details like date of airing, genre, tags, plot summary, cast, etc. and stats like no of reviewers, no of viewers, rating, rank, popularity along with the total number of positive and negative sentences used in the reviews, top 10 positive and negative review words as well location and gender info of the reviewers.

The script uses python's **Beautiful Soup** module for website scrapping and **"cardiffnlp/twitter-roberta-base-sentiment"** model from [Hugging Face](https://huggingface.co/) for sentimental analysis.


A sample of this dataset can be viewed on kaggle : [Top 500 Dramas from MyDramaList (+review details)](https://www.kaggle.com/datasets/anittasaju/top-500-dramas-from-mydramalist-reviewer-detail)


### Content

##### Scripts
*my_drama_list_scrapper.py*
This is the trigger script and imports all the relevant modules and sets the exceution flow. The output path for the dataset is also mentioned in this script.

##### SharedObjects
*shared_objects.py*
This script is imported to share all the global variables across modules. We have site_url and main_url variables in this script.

##### WebScrapper
*scrapping_web.py*
This module handles webscrapping actions and also to scrap a dictionary of all available dramas to scrap and thier links.  

##### ReviewProcessor
*review_scrapper.py*
This module is useful for scrapping reviews and thier reviewer profile details from a drama url.

*review_processor.py*
This module is useful for processing the reviews scrapped and also has the logic for creating bigrams.

##### ContentExtractor
*content_extractor.py*
This module takes care of extracting and processing general details scrapped from the webpages.


### Command to execute
cd Scripts
<br>python my_drama_list_scrapper.py

***Note1** :
<br>Output file is generate in the Scripts directory and is named as **Top_500_popular_drama_details_from_mydramalist.csv***

***Note2:**
<br>This script runs for top 500 dramas, in case you want to run the script for all the dramas in MyDramaList website, remove the code at line 106.
max_page = 25 #Change back*
