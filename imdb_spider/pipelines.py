# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html
import scrapy
from scrapy.pipelines.images import ImagesPipeline
from scrapy.exceptions import DropItem
from datetime import datetime,date,timedelta
import time

import MySQLdb as mysql_api
import MySQLdb.cursors

class DbPipeline(object):
	def __init__(self):
		self.mysql_conn = mysql_api.connect(host="localhost",
            port=3306, db="movie_vis", user="root", passwd="starcraft",
            charset='utf8',cursorclass=MySQLdb.cursors.DictCursor)
		self.mysql_cursor = self.mysql_conn.cursor()

	def process_item(self, item, spider):
		insert_movie_sql = """
		REPLACE INTO movie(title,director,actors,year,rating,short_intro,img_url) 
		VALUES('%s','%s','%s','%s','%s','%s','%s')"""%(item["title"], item["director"],
			item["actors"],item["year"], item["rating"], item["short_intro"], item['img_url'])
		print insert_movie_sql
		self.mysql_cursor.execute(insert_movie_sql)
		lastid = self.mysql_cursor.lastrowid

		for comment_item in item["comments"]:
			insert_comment_sql = """
			REPLACE INTO comment(title,comment,rating,date, movie_id) VALUES('%s','%s','%s','%s','%s')
			"""%(comment_item["title"],comment_item["comment"],comment_item["rating"],comment_item["date"],lastid)
			self.mysql_cursor.execute(insert_comment_sql)
		self.mysql_conn.commit()
		return item



class MovieImagesPipeline(ImagesPipeline):
	
	def file_path(self, request, response=None, info=None):
		image_guid = request.url.split('/')[-1]
		
		return 'full/%s' % (image_guid)


	def get_media_requests(self, item, info):
		for image_url in item['image_urls']:
			yield scrapy.Request(image_url, meta={"data":item})
	
	def item_completed(self, results, item, info):
		image_paths = [x['path'] for ok, x in results if ok]
		for im in image_paths:
			print im
		return item
