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
    tokenizedSent = word_tokenize(sentence)
    removedStopwords = [word.replace("_", " ").replace("/", " ") for word in tokenizedSent if word not in stopWords]
    finalOutput = []
    tempOutput = []
    for word in removedStopwords:
        if word not in set("``{['?/]}().,:-@") and word not in ["''"]:
            tempOutput.append(lemmatizer.lemmatize(word))
    finalOutput = " ".join(tempOutput)
    return finalOutput


def cosine_similarity_score(prompt_text, context_text):
    """Calculates cosine similarity between two text strings.

    Returns:
        float: The cosine similarity score between the two texts, as a percentage.
    """
    prompt_text, context_text = nlpTextProcessor(prompt_text), nlpTextProcessor(context_text)
    text_vectors = vectorizer.fit_transform([prompt_text, context_text])
    similarity_score = cosine_similarity(text_vectors)
    return similarity_score[0, 1]*100



# Testing the functions

# prompt = "slab wise sales"
# context = """slab_name"""


# print(nlpTextProcessor(context))
# print(cosine_similarity_score(prompt, context))