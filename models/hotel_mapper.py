import sqlite3
import os
class Hotel_Mapper():
	
	def __init__(self):
		self.source = os.path.expanduser('/Users/mac/Documents/GitHub/WuzzufTask/Task/models/hotelcrop.db')
	
	
	def set_hotel(self, Id, Name):
		conn = sqlite3.connect(self.source)
		c = conn.cursor()
		c.execute('INSERT INTO hotel VALUES (?,?)', (Id,Name))
		conn.commit()
		conn.close()
	
	
	def creat_db(self):
		conn = sqlite3.connect(self.source)
		c = conn.cursor()
		c.execute('''CREATE TABLE hotel(Id INTEGER NOT NULL, Name TEXT NOT NULL, PRIMARY KEY("Id"))''')
		conn.commit()
		conn.close()
		
	def get_hotel(self, Name):
		conn = sqlite3.connect(self.source)
		c = conn.cursor()
		c.execute('SELECT * FROM hotel WHERE Name=?', (Name,))
		res = c.fetchone()
		conn.commit()
		conn.close()
		return res