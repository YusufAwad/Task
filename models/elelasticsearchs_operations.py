import pandas as pd
import numpy as np
import json
import requests
from hotel_tone_analyzer import Hotel_Tone_Analyzer
from hotel_mapper import Hotel_Mapper
from elasticsearch import Elasticsearch

class ElelasticSearchs_Operations:
	
	def __init__(self):
		self.es = Elasticsearch([{'host':'localhost', 'port':9200}])
		
		self.hotel_dataframe = pd.read_csv('/Users/mac/Documents/GitHub/WuzzufTask/Task/data/7282_1.csv')
		self.hotel_dataframe = self.hotel_dataframe[self.hotel_dataframe['categories']=='Hotels']
		self.hotel_dataframe.drop(['reviews.doRecommend', 'reviews.id', 'reviews.userCity', 'reviews.userProvince'], axis = 1, inplace = True)
		self.hotel_dataframe.dropna(inplace = True)
		
		self.hotel_dataframe['reviews.date'] = pd.to_datetime(self.hotel_dataframe['reviews.date'])
		self.hotel_dataframe['reviews.dateAdded'] = pd.to_datetime(self.hotel_dataframe['reviews.dateAdded'])
		
		self.group_name = pd.DataFrame(self.hotel_dataframe)
		self.group_name.drop(['address', 'categories', 'city', 'country', 'latitude', 'longitude', 'postalCode', 'province'], axis = 1, inplace = True)

		self.group_name = self.group_name.groupby('name') 


		self.hotel_dataframe.drop_duplicates(subset = 'name', inplace = True) 
		self.hotel_dataframe.drop(['reviews.date', 'reviews.dateAdded', 'reviews.rating', 'reviews.rating', 'reviews.title', 'reviews.username', 'reviews.text'], axis = 1, inplace = True)
		self.dic_hotel = self.hotel_dataframe.to_dict(orient='records')
		
		self.hotel_map = Hotel_Mapper()
		
		
	def construct_tree(self):
		tone_analyzer = Hotel_Tone_Analyzer()
		url = 'http://localhost:9200/hotelcrop/_settings'
		headers = {'Content-Type': "application/json", 'Accept': "application/json"}
		
		try:
			self.es.indices.delete("hotelcrop")
#			print("yes")
		except:
			pass
			
		data = {}
		data['index.mapping.total_fields.limit'] = "10000"
		self.es.indices.create("hotelcrop")
		res = requests.put(url, json=data, headers=headers)
		i = 0
		self.hotel_map.creat_db()
		
		for row in self.dic_hotel:
			data = self.group_name.get_group(row['name'])
			j = 0
			
			for single_data in data.itertuples():
				row['review' + str(j)] = single_data
				j = j + 1
			
			row['tone analyzer'] = tone_analyzer.get_tone_analyzer(row['name'])
			try:
				res = self.es.index(index='hotelcrop',doc_type='hotel',id=j,body=row)
				self.hotel_map.set_hotel(i,row['name'])
				i = i + 1
			except:
#				print("error!, skiping chunk!")
				pass
			
		return {"Sucess": "Tree Constructed"}
			
			
	def retrieving_document(self, Name):
		try:
			select_hotel = self.hotel_map.get_hotel(Name)
			i = str(select_hotel[0])
			i = int(i)
			res = self.es.get(index='hotelcrop', doc_type = 'hotel', id = (i*1))
			return res
		
		except:
			return {"Error": "Name Not Found"}, 404
			
	
	def delete_document(self, Name):
		try:
			delete_hotel = self.hotel_map.get_hotel(Name)
			i = str(delete_hotel[0])
			i = int(i)
			res = self.es.delete(index='hotelcrop', doc_type = 'hotel', id = (i*1))
			return res
			
		except:
			return {"Error": "Name Not Found"}, 404