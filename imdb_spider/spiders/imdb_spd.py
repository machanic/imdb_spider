# -*- coding: utf-8 -*-
import scrapy
from scrapy.http import Request
from scrapy.http import Response
import re
import time
from imdb_spider.items import ImdbSpiderItem
import sys  
reload(sys)  
sys.setdefaultencoding('utf8')  


class ImdbSpdSpider(scrapy.Spider):
    name = "imdb_spd"
    allowed_domains = ["imdb.com"]
    start_urls = ['http://www.imdb.com/list/ls057823854/']
    def make_requests_from_url(self, url):
    	return Request(url, self.parse_list)

    def parse_list(self, response):
    	next_page_x = response.xpath("//div[@class='pagination']/a[contains(text(),'Next')]/@href").extract()
    	if len(next_page_x):
    		yield Request(url=response.urljoin(next_page_x[0]), callback=self.parse_list)

    	item_x = response.xpath("//div[@id='main']//div[@class='list_item odd'] | //div[@id='main']//div[@class='list_item even']")

    	for item in item_x:
    		try:
    			comment_url = "%sreviews?ref_=tt_ov_rt"%(response.urljoin(item.xpath(".//div[@class='info']/b/a/@href").extract()[0]))
    			
    			title = item.xpath(".//div[@class='info']/b/a/text()").extract()[0]
    			short_intro = item.xpath(".//div[@class='item_description']/text()").extract()[0]
    			director = item.xpath(".//div[@class='secondary'][contains(text(),'Director')]/a/text()").extract()[0].strip()
    			actors_x = item.xpath(".//div[@class='secondary'][contains(text(),'Star')]/a")
    			actors = []
    			for actor in actors_x:
    				actors.append(actor.xpath("./text()").extract()[0].strip())
    			image_url = item.xpath("./div[@class='image']//img/@src").extract()[0].strip()
    			year = item.xpath(".//span[@class='year_type']/text()").extract()[0][1:-1]
    			rating = item.xpath(".//span[@class='rating-rating']/span[@class='value']/text()").extract()[0]
    			
    			data = ImdbSpiderItem()
    			data["title"] = title.replace("'","''")
    			data["year"] = int(year[:4])
    			data["actors"] = " ; ".join(actors)
    			data["director"] = director
    			data["short_intro"] = short_intro.replace("'","''")
    			data["image_urls"] = [image_url]
    			data["img_url"] = image_url
    			data["rating"] = rating
    			data["comments"] = []
    			#print data,"redirecting to %s"%comment_url
    			#yield data
    			yield Request(url=comment_url, meta={"data":data},callback=self.parse_comment)
    		except Exception:
    			continue
    		
    	

    def parse_comment(self, response):
    	data = response.meta["data"]
    	
			#print other_comments_url
    	comment_x = response.xpath("//div[@id='tn15content']//p")
    	n = 0
    	for c in comment_x:
    		try:
	    		if (len(c.xpath(".//text()").extract()) == 0 or len(c.xpath("./preceding-sibling::div[1]//h2").extract()) == 0
	    			or len(c.xpath("./preceding-sibling::div[1]//img[@alt]").extract()) == 0):
	    			continue
	    		prev_div = c.xpath("./preceding-sibling::div[1]")
	    		date = prev_div.xpath("./small[last()]/text()").extract()[0].strip()
	    		date = time.strftime("%Y-%m-%d %H:%M:%S", time.strptime(date,'%d %B %Y'))  #store into mysql datetime
	    		comment_title = prev_div.xpath(".//h2/text()").extract()[0].strip()
	    		rating = prev_div.xpath(".//img[@alt]/@alt").extract()[0]
	    		rating = rating[:rating.rindex("/")]
	    		comment = re.sub(r"[\r\n]"," ", c.xpath(".//text()").extract()[0].strip())
	    		comment = re.sub(r"\\'","'", comment)
	    		if comment.startswith("***") and comment.endswith("***"):
	    			continue
	    		data["comments"].append({"title":comment_title.replace("'","''"), "rating":rating, "comment":comment.replace("'","''"), "date":date})
	    		#if len(data["comments"]) > 10:
	    		#	return data
	    		
	    		n+=1
	    	except Exception,e:
	    		raise e
    	print "process %s comments"%n
    	next_page = response.xpath("//table//img[@alt='[Next]']/parent::a/@href").extract()
    	if len(next_page) > 0:
    		return Request(url=response.urljoin(next_page[0]), meta={"data":data}, callback=self.parse_comment)
    	else:
    		return data
    	

 