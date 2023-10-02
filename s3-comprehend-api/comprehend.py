# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0

"""
Purpose

Shows how to use the AWS SDK for Python (Boto3) with Amazon Comprehend to
detect entities, phrases, and more in a document.
"""

# snippet-start:[python.example_code.comprehend.ComprehendDetect_imports]
import logging
from pprint import pprint
import boto3
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
# snippet-end:[python.example_code.comprehend.ComprehendDetect_imports]


# snippet-start:[python.example_code.comprehend.ComprehendDetect]
class ComprehendDetect:
    """Encapsulates Comprehend detection functions."""
    def __init__(self, comprehend_client):
        """
        :param comprehend_client: A Boto3 Comprehend client.
        """
        self.comprehend_client = comprehend_client

# snippet-end:[python.example_code.comprehend.ComprehendDetect]

# snippet-start:[python.example_code.comprehend.DetectDominantLanguage]
    def detect_languages(self, text):
        """
        Detects languages used in a document.

        :param text: The document to inspect.
        :return: The list of languages along with their confidence scores.
        """
        try:
            response = self.comprehend_client.detect_dominant_language(Text=text)
            languages = response['Languages']
            logger.info("Detected %s languages.", len(languages))
        except ClientError:
            logger.exception("Couldn't detect languages.")
            raise
        else:
            return languages
# snippet-end:[python.example_code.comprehend.DetectDominantLanguage]

# snippet-start:[python.example_code.comprehend.DetectSentiment]
    def detect_sentiment(self, text, language_code):
        """
        Detects the overall sentiment expressed in a document. Sentiment can
        be positive, negative, neutral, or a mixture.

        :param text: The document to inspect.
        :param language_code: The language of the document.
        :return: The sentiments along with their confidence scores.
        """
        try:
            response = self.comprehend_client.detect_sentiment(
                Text=text, LanguageCode=language_code)
            logger.info("Detected primary sentiment %s.", response['Sentiment'])
        except ClientError:
            logger.exception("Couldn't detect sentiment.")
            response = {'Sentiment': 'NIL', 'SentimentScore': {'Positive': 0, 'Negative': 0, 'Neutral': 0, 'Mixed': 0}, 'ResponseMetadata': {'RequestId': 'd64ed0aa-c8a3-4ca1-b6aa-7670e639537c', 'HTTPStatusCode': 200, 'HTTPHeaders': {'x-amzn-requestid': 'd64ed0aa-c8a3-4ca1-b6aa-7670e639537c', 'content-type': 'application/x-amz-json-1.1', 'content-length': '161', 'date': 'Fri, 29 Sep 000 11:31:39 GMT'}, 'RetryAttempts': 0}}
            return response
            # raise
        else:
            return response
# snippet-end:[python.example_code.comprehend.DetectSentiment]

# snippet-start:[python.example_code.comprehend.DetectSyntax]
    def detect_syntax(self, text, language_code):
        """
        Detects syntactical elements of a document. Syntax tokens are portions of
        text along with their use as parts of speech, such as nouns, verbs, and
        interjections.

        :param text: The document to inspect.
        :param language_code: The language of the document.
        :return: The list of syntax tokens along with their confidence scores.
        """
        try:
            response = self.comprehend_client.detect_syntax(
                Text=text, LanguageCode=language_code)
            tokens = response['SyntaxTokens']
            logger.info("Detected %s syntax tokens.", len(tokens))
        except ClientError:
            logger.exception("Couldn't detect syntax.")
            raise
        else:
            return tokens
# snippet-end:[python.example_code.comprehend.DetectSyntax]


# snippet-start:[python.example_code.comprehend.Usage_DetectApis]
def comprehend(id,comment):

    comp_detect = ComprehendDetect(boto3.client('comprehend'))
    # with open('dump.txt') as sample_file:
    #     sample_text = sample_file.read()

    demo_size = 3
    sample_text = comment
    print("Sample text used for this demo:")
    print('-'*88)
    print(sample_text)
    print('-'*88)

    print("Detecting languages.")
    languages = comp_detect.detect_languages(sample_text)
    pprint(languages)
    lang_code = languages[0]['LanguageCode']

    print("Detecting sentiment.")
    sentiment = comp_detect.detect_sentiment(sample_text, lang_code)
    print(f"Sentiment: {sentiment['Sentiment']}")
    print("SentimentScore:")
    pprint(sentiment['SentimentScore'])
    
    return({"id":id,"Comment":comment,"Sentiment":sentiment['Sentiment'],"SentimentScore":sentiment['SentimentScore']})

if __name__ == '__main__':
    comprehend()
