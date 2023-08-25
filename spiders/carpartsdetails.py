import scrapy
import sqlite3
import re
from time import sleep
import mysql.connector

class CarpartsdetailsSpider(scrapy.Spider):
    name = "carpartsdetails"
    allowed_domains = ["kosiski.autopartsearch.com"]
    # start_urls = ["https://kosiski.autopartsearch.com"]
    listing_url = "https://kosiski.autopartsearch.com/catalog-6/vehicle"

    # def parse(self, response):
    #     pass
    def start_requests(self):
        mysql_connection = mysql.connector.connect(
        host='localhost',
        port='3306',
        user='yoewalle_a2wp169',
        password='yoewalle_a2wp169_2020',
        database='kosiskiautopartsearch',
        )
        mysql_cursor = mysql_connection.cursor(dictionary=True)

        mysql_cursor.execute('SELECT * FROM url_table')
        fetched_data = mysql_cursor.fetchall()
        
        # print(fetched_data)
        mysql_cursor.close()
        mysql_connection.close()

        for data in fetched_data:
            url = data['url']
            maker = data['maker']  # Replace with the actual column names
            year = data['year']
            model = data['model']
            part = data['part']
            currentpage = data['currentpage']
            print("url", url)
            yield scrapy.Request(url=url, callback=self.parse_listing_page, 
                                  meta={"filter": (maker, year, model, part), "currentpage": 1})
            
    def parse_listing_page(self, response):

        print("Listing", response.url)

        car_data = response.meta["filter"]
        currentpage = int(response.meta["currentpage"])

        if response.status == 403:
            self.pause_for = 300
            sleep(self.pause_for)

            yield scrapy.Request(
                url=response.url,
                callback=self.parse_listing_page,
                # headers=self.headers,
                meta={"filter": car_data,
                      "currentpage": currentpage})
            self.pause_for = 0
            return

        partSections = response.selector.xpath('//*[@id="parts-table"]/form')

        for section in partSections:
            part = {}
            part["Car_Name"] = car_data[0]
            part["Car_Year"] = car_data[1]
            part["Car_Model"] = car_data[2]
            part["Car_Part"] = car_data[3]

            detail_href = section.xpath(
                './/a[contains(@href, "https://kosiski.autopartsearch.com/catalog-6/itemdetail")]/@href').get()
            part["Part_ID"] = detail_href.split('/')[-1]
            part["Part_href"] = detail_href
            part["Price"] = section.xpath(
                '//div[@class="buy-panel"]/ul/li/h3/text()').get()
            part["Yard_Notes"] = re.sub('\s+', ' ', ' '.join(
                section.xpath(
                    './/span[contains(text(), "Yard Notes:")]/following-sibling::strong//text() | .//span[contains(text(), "Yard Notes:")]/following-sibling::b//text()').getall())).strip()
            part["Application"] = re.sub('\s+', ' ', ' '.join(
                section.xpath(
                    './/span[contains(text(), "Application:")]/following-sibling::strong//text() | .//span[contains(text(), "Application:")]/following-sibling::b//text()').getall())).strip()
            part["Body_Color"] = re.sub('\s+', ' ', ' '.join(
                section.xpath(
                    './/span[contains(text(), "Body Color:")]/following-sibling::strong//text() | .//span[contains(text(), "Body Color:")]/following-sibling::b//text()').getall())).strip()
            part["Vehicle"] = re.sub('\s+', ' ', ' '.join(
                section.xpath(
                    './/span[contains(text(), "Vehicle:")]/following-sibling::strong//text() | .//span[contains(text(), "Vehicle:")]/following-sibling::b//text()').getall())).strip()
            part["Warehouse_ID"] = re.sub('\s+', ' ', ' '.join(
                section.xpath(
                    './/span[contains(text(), "Warehouse ID:")]/following-sibling::strong//text() | .//span[contains(text(), "Warehouse ID:")]/following-sibling::b//text()').getall())).strip()
            part["Year"] = re.sub('\s+', ' ', ' '.join(
                section.xpath(
                    './/span[contains(text(), "Year:")]/following-sibling::strong//text() | .//span[contains(text(), "Year:")]/following-sibling::b//text()').getall())).strip()
            part["Status"] = re.sub('\s+', ' ', ' '.join(
                section.xpath(
                    './/span[contains(text(), "Status:")]/following-sibling::strong//text() | .//span[contains(text(), "Status:")]/following-sibling::b//text()').getall())).strip()
            part["Condition"] = re.sub('\s+', ' ', ' '.join(
                section.xpath(
                    './/span[contains(text(), "Condition:")]/following-sibling::strong//text() | .//span[contains(text(), "Condition:")]/following-sibling::b//text()').getall())).strip()
            part["Mileage"] = re.sub('\s+', ' ', ' '.join(
                section.xpath(
                    './/span[contains(text(), "Mileage:")]/following-sibling::strong//text() | .//span[contains(text(), "Mileage:")]/following-sibling::b//text()').getall())).strip()
            yield part

        next_page_href = response.selector.xpath(f'.//a[contains(@href, "currentpage={currentpage + 1}")]/@href').get()
        if next_page_href and len(next_page_href) > 0:
            yield scrapy.Request(
                url=next_page_href,
                callback=self.parse_listing_page,
                headers=self.headers,
                meta={"filter": car_data,
                      "currentpage": currentpage + 1})
