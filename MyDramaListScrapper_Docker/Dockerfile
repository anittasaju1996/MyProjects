FROM python:3.8-slim-bullseye

WORKDIR /home

RUN mkdir MyDramaListScrapper Output_files nltk_data

#ADD MyDramaListScrapper  MyDramaListScrapper/

COPY requirement.txt .
RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install -r requirement.txt
RUN python3 -m nltk.downloader -d /home/nltk_data all

RUN mkdir log_files

#CMD ["tail", "-f", "/dev/null"]
#CMD ["cd", "/home/MyDramaListScrapper/Scripts", ";", "python3", "my_drama_list_scrapper.py", ">", "/home/log_files/log.txt"]