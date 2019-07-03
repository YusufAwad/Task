from flask_restful import Resource, reqparse, Api
from models.elelasticsearchs_operations import ElelasticSearchs_Operations

class ConstructTree(Resource):
	def post(self):
		elelastic_operation = ElelasticSearchs_Operations()
		return elelastic_operation.construct_tree()
		
class DeleteDocument(Resource):
	parser = reqparse.RequestParser()
	parser.add_argument('Id',
						type=int,
						required=True,
						help="This field cannot be blank."
						)
	def post(self):
		data = UserRegister.parser.parse_args()
		elelastic_operation = ElelasticSearchs_Operations()
		return elelastic_operation.delete_document(data['Id'])
		
class SelectDocument(Resource):
	def get(self, Id):
		elelastic_operation = ElelasticSearchs_Operations()
		return elelastic_operation.retrieving_document(Id)