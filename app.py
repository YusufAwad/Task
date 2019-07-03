from flask import Flask, render_template
from flask_restful import Api
import sys, os
sys.path.append('/Users/mac/Documents/GitHub/WuzzufTask/Task/models')



from resources.hotel_tone_analyzer import GetToneAnalyzer
from resources.elelasticsearchs_operations import ConstructTree
from resources.elelasticsearchs_operations import DeleteDocument
from resources.elelasticsearchs_operations import SelectDocument
from resources.elelasticsearchs_operations import SelectDocument
from resources.hotel_mapper import GetHotelId




app = Flask(__name__)
app.config['PROPAGATE_EXCEPTIONS'] = True
api = Api(app)



api.add_resource(GetToneAnalyzer, '/GetToneAnalyzer/<string:Name>')
api.add_resource(ConstructTree, '/ConstructTree')
api.add_resource(DeleteDocument, '/DeleteDocument')
api.add_resource(SelectDocument, '/SelectDocument/<int:Id>')



if __name__ == '__main__':
	app.run(port=5000, debug=True)
