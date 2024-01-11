import json
# open json file
with open('words_sentiment.json') as f:
    words_sentiment = json.load(f)

"""
Each word has this values:

"anger": 0,
"anticipation": 0,
"disgust": 0,
"fear": 0,
"joy": 0,
"negative": 0,
"positive": 1,
"sadness": 0,
"surprise": 0,
"trust": 1

"""

positive_scores = [
    "anticipation",
    "joy",
    "positive",
    "surprise",
    "trust"
]

negative_scores = [
    "anger",
    "disgust",
    "fear",
    "negative",
    "sadness"
]

sentiment_dict = {
    "positive": [],
    "negative": [],
    "neutral": []
}

for word, value in words_sentiment.items():
    pos, neg = 0, 0
    for score, val in value.items():
        if score in positive_scores:
            pos += val
        elif score in negative_scores:
            neg += val

    if pos > neg:
        sentiment_dict["positive"].append(word)
    elif neg > pos:
        sentiment_dict["negative"].append(word)
    else:
        sentiment_dict["neutral"].append(word)