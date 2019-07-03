import pandas as pd
import numpy as np
from ibm_watson import ToneAnalyzerV3

class Hotel_Tone_Analyzer:
	
	def __init__(self):
		# 1- Read dataframe
		# 2- Get the hotels only from the dataframe
		# 3- Set Watson ton analyzer connectio *NOTE I choose 2016-05-19 version because 2017-09-21 did not return all the Emotion Tones it only return the highest one and from what I understood you wanted the prectenge of all the tones 
		self.hote_dataframe = pd.read_csv('/Users/mac/Documents/GitHub/WuzzufTask/Task/data/7282_1.csv')
		self.hote_dataframe = self.hote_dataframe[self.hote_dataframe['categories']=='Hotels']
#		self.hote_dataframe = self.hote_dataframe[self.hote_dataframe['reviews.text'] != '']
		self.hote_dataframe.dropna(subset=['reviews.text'], inplace = True)
		self.tone_analyzer = ToneAnalyzerV3(
			version='2016-05-19',
			iam_apikey='fONibKqWoy-MuVSzwCy_ZEIocjeWSr9DbjZPVCxESKiE',
			url='https://gateway-lon.watsonplatform.net/tone-analyzer/api'
		)
		
	
	
	def	get_tone_analyzer(self, name):
		# Allocing all the rows of a specific hotel and empty string that will agaraget all the reviwes   
		
		hotel_review = self.hote_dataframe[self.hote_dataframe['name'] == name]
		
		text = ''
		
		# Agaragetting and removing the punctuations as I found Watson count the end of a sentenc with every character of these I treid to use nltk tokenizer lib but did not work
		for review in hotel_review['reviews.text']:
			review = review.replace(".", "")
			review = review.replace(",", "")
			review = review.replace("-", "")
			review = review.replace("!", "")
			# As the limit of Watson Toner is 128kb
			if(sys.getsizeof(text + review) > 128000):
				break
			text = text + review + '\n'

			
		# Sending the reviews to tone analyzer and returned in a form of JSON
		# Checking if the review exceede the tone analyzer limit. From what I found its limit is 128 kb and only about five hotel that its reviews exceeds the limit
		try:
			tone_analysis = self.tone_analyzer.tone(
				{'text': text},
				content_type='application/json'
			).get_result()
			# Getting the document tone. The Document Emotion Tone to be specific
			result = tone_analysis['document_tone']['tone_categories'][0]
		
			# Dictionary that will have the tone name and its percentage
			json_data = dict()
		
			for tone in result['tones']:
				json_data[tone['tone_name']] = tone['score']
			
			return json_data
		except:
			return {"Text exceeded the limit": 0}
		
		