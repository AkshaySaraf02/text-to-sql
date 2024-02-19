from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from nltk.stem import WordNetLemmatizer
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import nltk

nltk.download('stopwords')
nltk.download('punkt')
nltk.download('wordnet')
stopWords = set(stopwords.words("english"))

lemmatizer = WordNetLemmatizer()
vectorizer = TfidfVectorizer()


def nlpTextProcessor(sentence : str):
    """Preprocesses a text string to filter out words that does not provide much information.

    Returns:
        str: The preprocessed text string, with stop words removed,
             punctuation and extra characters filtered out, and words lemmatized.
    """
    tokenizedSent = word_tokenize(sentence.replace("_", " ").replace("/", " "))
    removedStopwords = [word for word in tokenizedSent if word not in stopWords]
    finalOutput = []
    tempOutput = []
    for word in removedStopwords:
        if word not in set("``{['?/]}().,:-@") and word not in ["''"]:
            tempOutput.append(lemmatizer.lemmatize(word))
    finalOutput = " ".join(tempOutput)
    return finalOutput


def cosine_similarity_scores(prompt_text, context_text, threshold, output_list, label="", matching_key="", name_key=""):
    """Calculates cosine similarity between two text strings.

    Returns:
        float: The cosine similarity score between the two texts, as a percentage.
    """
    print("\n" + label)
    if type(context_text) == str:
        prompt_text, context_text = nlpTextProcessor(prompt_text.lower()), nlpTextProcessor(context_text.lower())
        text_vectors = vectorizer.fit_transform([prompt_text, context_text])
        similarity_score = cosine_similarity(text_vectors)
        return round(similarity_score[0, 1]*100, 2)
    else:
        for item in context_text:
            item_dict = item
            if len(matching_key) > 0:
                item = item[matching_key]
            prompt_text, item = str(prompt_text), str(item)
            prompt_text, item = nlpTextProcessor(prompt_text.lower()), nlpTextProcessor(item.lower())
            text_vectors = vectorizer.fit_transform([prompt_text, item])
            similarity_score = cosine_similarity(text_vectors)    
            score = round(similarity_score[0, 1]*100, 2)
            print(item_dict[name_key], ": ", score, "%")
                
            if score >= threshold and item_dict not in output_list:
                output_list.append(item_dict)


# Testing the functions

# prompt = "slab wise sales"
# context = """slab_name"""


# print(nlpTextProcessor(context))
# print(cosine_similarity_score(prompt, context))