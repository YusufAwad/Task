import pandas as pd
import numpy as np
import json
import requests
from hotel_tone_analyzer import Hotel_Tone_Analyzer
from elasticsearch import Elasticsearch

class ElelasticSearchs_Operations:
	
	def __init__(self):
		self.es = Elasticsearch([{'host':'localhost', 'port':9200}])
		
		self.hotel_dataframe = pd.read_csv('/Users/mac/Documents/GitHub/WuzzufTask/Task/data/7282_1.csv')
		self.hotel_dataframe = self.hotel_dataframe[self.hotel_dataframe['categories']=='Hotels']
		# Dropping the Null Cloumns as I found all of its value are equal to null form kaggle
		self.hotel_dataframe.drop(['reviews.doRecommend', 'reviews.id', 'reviews.userCity', 'reviews.userProvince'], axis = 1, inplace = True)
		# Drop any record that has null value
		self.hotel_dataframe.dropna(inplace = True)
		
		# Casting the dates into datetime data as for the date detection in the Elasticsearch
		self.hotel_dataframe['reviews.date'] = pd.to_datetime(self.hotel_dataframe['reviews.date'])
		self.hotel_dataframe['reviews.dateAdded'] = pd.to_datetime(self.hotel_dataframe['reviews.dateAdded'])
		
		# Creating new dataset to group every hotle revives together 
		self.group_name = pd.DataFrame(self.hotel_dataframe)
		self.group_name.drop(['address', 'categories', 'city', 'country', 'latitude', 'longitude', 'postalCode', 'province'], axis = 1, inplace = True)

		self.group_name = self.group_name.groupby('name') 
		
		# remove the duplication record after separating each group of reviews for a single hotle in a single dataframe
		self.hotel_dataframe.drop_duplicates(subset = 'name', inplace = True) 
		self.hotel_dataframe.drop(['reviews.date', 'reviews.dateAdded', 'reviews.rating', 'reviews.rating', 'reviews.title', 'reviews.username', 'reviews.text'], axis = 1, inplace = True)
		self.dic_hotel = self.hotel_dataframe.to_dict(orient='records')
		
		
		
	def construct_tree(self):
		# Setting up the tone analyzer to get the tone for every hotel and index with its data in the tree
		tone_analyzer = Hotel_Tone_Analyzer()
		# Setting up Elasticsearch connection
		url = 'http://localhost:9200/hotelcrop/_settings'
		headers = {'Content-Type': "application/json", 'Accept': "application/json"}
		# If an index exists delete and creat new one then send a requst to increase the field limit
		try:
			self.es.indices.delete("hotelcrop")
#			print("yes")
		except:
			pass
			
		data = {}
		data['index.mapping.total_fields.limit'] = "10000"
		self.es.indices.create("hotelcrop")
		res = requests.put(url, json=data, headers=headers)
		
		# Indexing each hotel document in the index. Each document coutain all the data for each hotel no duplication 
		i = 0
		
		for row in self.dic_hotel:
			data = self.group_name.get_group(row['name'])
			j = 0
			
			for single_data in data.itertuples():
				row['review' + str(j)] = single_data
				j = j + 1
			
			row['tone analyzer'] = tone_analyzer.get_tone_analyzer(row['name'])
			try:
				res = self.es.index(index='hotelcrop',doc_type='hotel',id=j,body=row)
				i = i + 1
			except:
#				print("error!, skiping chunk!")
				pass
			
		return {"Sucess": "Tree Constructed"}
			
		
	def retrieving_document(self, Id):
		try:
			res = self.es.get(index='hotelcrop', doc_type = 'hotel', id = Id)
			return res
		
		except:
			return {"Error": "Name Not Found"}, 404
			
	
	def delete_document(self, Id):
		try:
			res = self.es.delete(index='hotelcrop', doc_type = 'hotel', id = Id)
			return res
			
		except:
			return {"Error": "Name Not Found"}, 404