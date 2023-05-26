# Import python packages
import nltk
nltk.data.path.append("/home/nltk_data")
from nltk.corpus import stopwords
from nltk.stem import WordNetLemmatizer
from nltk.corpus import wordnet
import re
from collections import Counter
from transformers import AutoTokenizer
# Import custom packages
from ReviewProcessor.review_scrapper import review_parsing, extract_page_reviews_v2, extract_reviewer_profiles_links
from SharedObjects.shared_objects import *
from WebScrapper.scrapping_web import extract_url_details, function_threading


def pos_tagger(nltk_tag):
    """
    Function is used to convert nltk tag to wordnet tag
    IP : nltk tag
    OP : wordnet tag
    """
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


def extract_sentence_trigram_v2(sentence, sw):
    """
    Function is used to convert a sentence into a trigram and bigram lists
    IP : sentence and stopword list
    OP : list of bigram and trigram
    Error : Has to be handled by parent
    """
    lst_of_words = []
    lemmatizer = WordNetLemmatizer()  # Initializing lemmatizer
    lemmatized_sentence = []

    # to remove stop words from the sentences
    for word in nltk.word_tokenize(sentence.lower()):
        if word in sw:
            continue
        lst_of_words.append(word)

    # deriving pos tag to properly lemmatized the words as per pos tags
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

        # For 0,1,2 are positions in a trigram, to remove repetition of words following logic is used
        # If 0 == 1 == 2 then remove from list => Handled only in trigram
        # If 0 == 1 and 1 != 2 then bigram 1,2 => handled in bigram
        # If 0 != 1 and 0 == 2 then bigram 0,1 => handles in bigram
        # If 0 != 1 and 1 != 2 only then trigram is made

        trigram_lst = []
        for item in nltk.trigrams(lemmatized_sentence):
            if item[0] != item[1] and item[1] != item[2] and item[0] != item[
                2]:  # Only if all there are not equal then trigram is made
                trigram_lst.append(" ".join(item))

        return [bigram_lst, trigram_lst]  # a list of both bigram and trigram are returned

    else:
        return []


def extract_trigram_v3(review_text_lst, bi_cnter, tri_cnter, sw):
    """
    Function is called to get counter of bigrams and trigrams from the list of sentences passed
    IN : list of sentences, bigram counter, trigrams counter and stop words
    OP : no return, info is updated in the passed bigram and trigram counters
    Error : has to be handled by parent
    """
    # review_trigram_cnter = Counter()
    for sentence in review_text_lst:

        gram_lst = extract_sentence_trigram_v2(sentence,
                                               sw)  # Passing sentence to this function to get its bigram and trigram
        # If after lemmatization the sentence has <= 2 words then only bigrams are passed and len of gram_lst is 1
        if len(gram_lst) == 1:
            bi_cnter.update(gram_lst[0])
        elif len(gram_lst) == 2:
            bi_cnter.update(gram_lst[0])
            tri_cnter.update(gram_lst[1])
        else:
            pass


def processing_cnters(bigram_cnter, trigram_cnter, output_words=10):
    """
    Function is used to get top 10 people sentiment from the list of bigrams and trigram  provided
    IP : bigram and trigram counter and the number of words we want
    OP : top 10 words used if available
    Error : handled by parent

    Logic used    :
    For trigrams swapped would be handled by - suffixes
    Assumed that only one word of the trigram would be swapped as prefix or suffix
    eg : grim reaper sunny and sunny grim reaper would be  : grim reaper - sunny
    If the bigram is not appearing in trigram, then keep the bigram in its order
    If bigram appearing in trigram then collect all the trigrams with this bigram, then
    Collect prefixes
    Collect suffixes
    Change bigram to => bigram - set(suffixes[s] + prefixe[s]) #Only specify first 5 prefix/suffix for a bigram
    Delete all the bigrams with prefixes and suffixes and trigrams
    """
    # One main assumption here is that any gram occurring less than 3 times is not providing any useful info
    bigram_cnt = min([len(list(filter(lambda x: x > 2, bigram_cnter.values()))),
                      output_words * 2])  # As there would be reduction, taking a subset of double the words required to work with, i.e output_words * 2
    trigram_cnt = min([len(list(filter(lambda x: x > 2, trigram_cnter.values()))),
                       bigram_cnt * 2])  # As trigram are less frequent to coincide, taking range as double than the bigram
    bigram_swap_cnt = min([len(list(filter(lambda x: x > 2, bigram_cnter.values()))),
                           bigram_cnt * 2])  # As there would be reduction, taking a range double than requereied , bigram_cnt * 2

    print(f"Bigram cnt considered : {bigram_cnt}")

    # Removing swapped text from the bigrams : eg. sunny reaper and reaper sunny only one with high occurrence has to be retained
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

            # Keep bigram, and search if any suffixes and prefixes are available in the top trigrams

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

                # only add the prefix/suffix if it is already not in the list and using a list here to preserve the order of rnk in trigram
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

            # join the suffix/prefix to the main bigram in format : main bigram - (semicolon separated suffix and prefix in rnk ordering)
            if prefix_suffix_lst:
                new_word = bi_text + " - (" + "; ".join(prefix_suffix_lst) + ")"
                bigram_final_lst.append(new_word)  # ((new_word, bi_cnt))

    return bigram_final_lst[:output_words]


def extract_reviews_to_sentences(url, page_dict, set_reviewer_profile_links):
    """
    Function is used to parse reviews and return them as sentences
    Is also used to gather profile info of the drama reviewers
    IP : review url, dict to store info and set to store reviewer profile info
    OP : extracted sentences are returned and reviewer info is stored in the dict that is passed
    Error : Has to be handled by parent
    """
    # Starting review parsing
    all_reviews = []  # To store all the reviews
    reviewer_profile_links = []  # To store all reviewer profile urls

    review_url = url + "/reviews?xlang=en-US"  # Review url for this drama
    review_doc, max_review_page = extract_url_details(review_url, check_max_page=True)

    if not review_doc:
        raise Exception(f"Not able to access review url : {review_url}")

    print(f"Drama has {max_review_page} review pages")

    print("Starting to extract reviews")

    try:
        extract_page_reviews_v2(review_doc,
                                all_reviews)  # Extracting page review for first page as already available
        extract_reviewer_profiles_links(review_doc, site_url,
                                        reviewer_profile_links)  # Extracting reviewer profile url for first page

        if max_review_page > 1:
            # After threading
            function_threading(range(2, max_review_page + 1), review_parsing,
                               (review_url, all_reviews, reviewer_profile_links))  # took 3.98 secs

        set_reviewer_profile_links.update(reviewer_profile_links)
        page_dict["reviewer_profile_links"] = set(reviewer_profile_links)

        page_dict["no_of_extracted_reviews"] = len(all_reviews)

        print("\nAll reviews have been extracted")

        all_sentences = []
        tokenizer = AutoTokenizer.from_pretrained("cardiffnlp/twitter-roberta-base-sentiment")
        # Converting list of (list of sentences for a review) into a list of all sentences after retaining meaningful sentences
        for list_sentence in all_reviews:
            for sentence in list_sentence:
                s = " ".join(re.split(r"\W+", sentence))
                s = s.strip()
                if s:
                    tokens = tokenizer(s)["input_ids"][1:-1] #Removing <s> and </s> token ids (0 and 2) from lst
                    for i in range(0, len(tokens), 500):
                        all_sentences.append(tokenizer.decode(tokens[i:i+500])) #Setting limit of 500 tokens per sentence, as above 512 tokens the model fails

        page_dict["Total_sentences"] = len(all_sentences)

        return all_sentences

    except Exception as e:
        raise Exception("Not able to parse review pages properly, error is : " + str(e))


def create_grams(all_sentences, lst_categories, page_dict):
    """
    Function is used to gather top 10 +ve and -ve word groupings if available from the sentences
    It ranks bigrams and also adds suffixes or prefixes commonly used with them
    IP : sentences to parse, category of sentence in order, dict to update info
    OP : No return, info is stored in the dict that is passed
    Error : Has to be handled by parent
    """
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

    positive_sentences = []
    negative_sentences = []


    for category in lst_categories:
        if not category:
            pass
        elif category["label"] == "POSITIVE":
            positive_sentences.append(all_sentences[category["position"]])
        else:
            negative_sentences.append(all_sentences[category["position"]])

    print("Starting with bigram and trigram extractions from reviews")
    # extracting trigram and bigram from drama content to remove them before raking
    content_bigram = Counter()
    content_trigram = Counter()

    extract_trigram_v3(re.split(r"[.\n\r]", page_dict["content"]), content_bigram, content_trigram, sw)

    # Making their counts 999 ~ infy to remove their counts from counter easily
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

        # Calling function which ranks the top 10 word associations
        label_sentiment = processing_cnters(bigram_counter, trigram_counter)
        page_dict[label + "_people_sentiment"] = ", ".join(label_sentiment)
        page_dict[label + "_sentences"] = len(lst_sentence)  # Also adding total number of words

