from flask_restful import Resource, reqparse
from models.hotel_tone_analyzer import Hotel_Tone_Analyzer


class GetToneAnalyzer(Resource):
	
	def get(self, Name):	
		tone_analyzer = Hotel_Tone_Analyzer()
		data = tone_analyzer.get_tone_analyzer(Name)
		return data
		
		
	