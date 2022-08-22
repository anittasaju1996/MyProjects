{
 "cells": [
  {
   "cell_type": "markdown",
   "id": "1942a8c7",
   "metadata": {},
   "source": [
    "### About this project\n",
    "This project is used to scrap information of all the dramas in [MyDramaList](https://mydramalist.com/) website. \n",
    "\n",
    "The dataset generated is a csv that consists of all completed (Drama + Drama Special) from MyDramaList and covers drama across multiple countries like South Korea, China, Japan, Thailand, Taiwan, etc. It gives a comprehensive report of all general details like date of airing, genre, tags, plot summary, cast, etc. and stats like no of reviewers, no of viewers, rating, rank, popularity along with the total number of positive and negative sentences used in the reviews, top 10 positive and negative review words as well location and gender info of the reviewers.\n",
    "\n",
    "The script uses python's **Beautiful Soup module** for website scrapping and **\"cardiffnlp/twitter-roberta-base-sentiment\"** model from [Hugging Face](https://huggingface.co/) for sentimental analysis.\n",
    "\n",
    "\n",
    "A sample of this dataset can be viewed on Kaggle : [Top 500 Dramas from MyDramaList (+review details)](https://www.kaggle.com/datasets/anittasaju/top-500-dramas-from-mydramalist-reviewer-detail)\n",
    "\n",
    "\n",
    "### Content\n",
    "\n",
    "#### Scripts\n",
    "*my_drama_list_scrapper.py*\n",
    "<br>This is the trigger script and imports all the relevant modules and sets the exceution flow. The output path for the dataset is also mentioned in this script.\n",
    "\n",
    "#### SharedObjects\n",
    "*shared_objects.py*\n",
    "<br>This script is imported to share all the global variables across modules. We have site_url and main_url variables in this script.\n",
    "\n",
    "#### WebScrapper\n",
    "*scrapping_web.py*\n",
    "<br>This module handles webscrapping actions and also to scrap a dictionary of all available dramas to scrap and thier links.  \n",
    "\n",
    "#### ReviewProcessor\n",
    "*review_scrapper.py*\n",
    "<br>This module is useful for scrapping reviews and thier reviewer profile details from a drama url.\n",
    "\n",
    "*review_processor.py*\n",
    "<br>This module is useful for processing the reviews scrapped and also has the logic for creating bigrams.\n",
    "\n",
    "\n",
    "#### ContentExtractor\n",
    "*content_extractor.py*\n",
    "<br>This module takes care of extracting and processing general details scrapped from the webpages."
   ]
  }
 ],
 "metadata": {
  "kernelspec": {
   "display_name": "Python 3 (ipykernel)",
   "language": "python",
   "name": "python3"
  },
  "language_info": {
   "codemirror_mode": {
    "name": "ipython",
    "version": 3
   },
   "file_extension": ".py",
   "mimetype": "text/x-python",
   "name": "python",
   "nbconvert_exporter": "python",
   "pygments_lexer": "ipython3",
   "version": "3.8.10"
  }
 },
 "nbformat": 4,
 "nbformat_minor": 5
}
