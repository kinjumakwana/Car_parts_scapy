import re
from time import sleep

import scrapy
from scrapy_selenium import SeleniumRequest
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.select import Select
from selenium.webdriver.support.wait import WebDriverWait
import sqlite3
from scrapy.crawler import CrawlerProcess
from .carpartsdetails import CarpartsdetailsSpider
from selenium import webdriver
    
class CarPartsSpider(scrapy.Spider):
    name = "carpartsspider"
    allowed_domains = ["kosiski.autopartsearch.com"]
    start_url = "https://kosiski.autopartsearch.com"
    listing_url = "https://kosiski.autopartsearch.com/catalog-6/vehicle"
    pause_for = 0
    headers = {
        'authority': 'kosiski.autopartsearch.com',
        'accept': '*/*',
        'accept-language': 'en-US,en;q=0.9',
        'cache-control': 'no-cache',
        'content-type': 'application/json',
        'origin': 'https://kosiski.autopartsearch.com',
        'Referer': 'https://kosiski.autopartsearch.com',
        'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/96.0.4664.93 Safari/537.36',
    }

    def start_requests(self):
        yield SeleniumRequest(url=self.start_url,
                              headers=self.headers,
                              callback=self.parse_home_page)

    def parse_home_page(self, response):
        print("parse_home_page")
        print("URL:", response.url)
        total_parts = 0

        driver = response.meta["driver"]
    
        selectMake = Select(WebDriverWait(driver, 10).until(
            EC.presence_of_element_located((By.XPATH, '//select[@id="afmkt-make"]'))))
        
        print("selectMake",selectMake)
        try:
            if WebDriverWait(driver, 5).until(
                    EC.presence_of_element_located((By.XPATH, '//select[@id="afmkt-make"]/option'))):

                makerValues = [option.get_attribute('value') for option in selectMake.options]

                for maker in makerValues:
                    if maker:
                        selectMake.select_by_value(maker)
                        sleep(1)
                        sleep(self.pause_for)

                        selectYear = Select(WebDriverWait(driver, 10).until(
                            EC.presence_of_element_located((By.XPATH, '//select[@id="afmkt-year"]'))))

                        try:
                            if WebDriverWait(driver, 5).until(
                                    EC.presence_of_element_located((By.XPATH, '//select[@id="afmkt-year"]/option'))):
                                yearValues = [option.get_attribute('value') for option in selectYear.options]

                                for year in yearValues:
                                    if year:
                                        selectYear.select_by_value(year)
                                        sleep(1)
                                        sleep(self.pause_for)

                                        selectModel = Select(WebDriverWait(driver, 10).until(
                                            EC.presence_of_element_located((By.XPATH, '//select[@id="afmkt-model"]'))))

                                        try:
                                            if WebDriverWait(driver, 5).until(
                                                    EC.presence_of_element_located(
                                                        (By.XPATH, '//select[@id="afmkt-model"]/option'))):
                                                modelValues = [option.get_attribute('value') for option in
                                                               selectModel.options]

                                                for model in modelValues:
                                                    if model:
                                                        selectModel.select_by_value(model)
                                                        sleep(2)
                                                        sleep(self.pause_for)

                                                        selectParts = Select(WebDriverWait(driver, 10).until(
                                                            EC.presence_of_element_located(
                                                                (By.XPATH, '//select[@id="own-parttype"]'))))

                                                        try:
                                                            if WebDriverWait(driver, 5).until(
                                                                    EC.presence_of_element_located(
                                                                        (By.XPATH,
                                                                         '//select[@id="own-parttype"]/option'))):
                                                                partValues = [option.get_attribute('value') for option
                                                                              in
                                                                              selectParts.options]

                                                                for part in partValues:
                                                                    if part:
                                                                        print(part)
                                                                        total_parts += 1
                                                                        print(total_parts, "|", maker, "|", year, "|",
                                                                              model, "|", part)
                                                                        selectParts.select_by_value(part)
                                                                        # yield scrapy.Request(
                                                                        #     url=f"{self.listing_url}/{maker}/{year}/{model}/{part}",
                                                                        #     callback=self.parse_listing_page,
                                                                        #     headers=self.headers,
                                                                        #     meta={"filter": (maker, year, model, part),
                                                                        #           "currentpage": 1})
                                                                        url = f"{self.listing_url}/{maker}/{year}/{model}/{part}"
                                                                        print(url)
                                                                        yield {
                                                                            'url': url,
                                                                            'maker': maker,
                                                                            'year': year,
                                                                            'model': model,
                                                                            'part': part,
                                                                            'currentpage': 1
                                                                        }
                                                                        sleep(self.pause_for)
                                                                      
                                                                        # Create a CrawlerProcess
                                                                        # process = CrawlerProcess()

                                                                        # # Add CarpartsdetailsSpider to the process
                                                                        # process.crawl(CarpartsdetailsSpider)

                                                                        # # Start the crawling process
                                                                        # process.start()
                                                                
                                                        except:
                                                            pass
                                        except:
                                            pass
                        except:
                            pass
        except:
            pass

    # def parse_listing_page(self, response):

    #     print("Listing", response.url)

    #     car_data = response.meta["filter"]
    #     currentpage = int(response.meta["currentpage"])

    #     if response.status == 403:
    #         self.pause_for = 300
    #         sleep(self.pause_for)

    #         yield scrapy.Request(
    #             url=response.url,
    #             callback=self.parse_listing_page,
    #             headers=self.headers,
    #             meta={"filter": car_data,
    #                   "currentpage": currentpage})
    #         self.pause_for = 0
    #         return

    #     partSections = response.selector.xpath('//*[@id="parts-table"]/form')

    #     for section in partSections:
    #         part = {}
    #         part["Car_Name"] = car_data[0]
    #         part["Car_Year"] = car_data[1]
    #         part["Car_Model"] = car_data[2]
    #         part["Car_Part"] = car_data[3]

    #         detail_href = section.xpath(
    #             './/a[contains(@href, "https://kosiski.autopartsearch.com/catalog-6/itemdetail")]/@href').get()
    #         part["Part_ID"] = detail_href.split('/')[-1]
    #         part["Part_href"] = detail_href
    #         part["Price"] = section.xpath(
    #             '//div[@class="buy-panel"]/ul/li/h3/text()').get()
    #         part["Yard_Notes"] = re.sub('\s+', ' ', ' '.join(
    #             section.xpath(
    #                 './/span[contains(text(), "Yard Notes:")]/following-sibling::strong//text() | .//span[contains(text(), "Yard Notes:")]/following-sibling::b//text()').getall())).strip()
    #         part["Application"] = re.sub('\s+', ' ', ' '.join(
    #             section.xpath(
    #                 './/span[contains(text(), "Application:")]/following-sibling::strong//text() | .//span[contains(text(), "Application:")]/following-sibling::b//text()').getall())).strip()
    #         part["Body_Color"] = re.sub('\s+', ' ', ' '.join(
    #             section.xpath(
    #                 './/span[contains(text(), "Body Color:")]/following-sibling::strong//text() | .//span[contains(text(), "Body Color:")]/following-sibling::b//text()').getall())).strip()
    #         part["Vehicle"] = re.sub('\s+', ' ', ' '.join(
    #             section.xpath(
    #                 './/span[contains(text(), "Vehicle:")]/following-sibling::strong//text() | .//span[contains(text(), "Vehicle:")]/following-sibling::b//text()').getall())).strip()
    #         part["Warehouse_ID"] = re.sub('\s+', ' ', ' '.join(
    #             section.xpath(
    #                 './/span[contains(text(), "Warehouse ID:")]/following-sibling::strong//text() | .//span[contains(text(), "Warehouse ID:")]/following-sibling::b//text()').getall())).strip()
    #         part["Year"] = re.sub('\s+', ' ', ' '.join(
    #             section.xpath(
    #                 './/span[contains(text(), "Year:")]/following-sibling::strong//text() | .//span[contains(text(), "Year:")]/following-sibling::b//text()').getall())).strip()
    #         part["Status"] = re.sub('\s+', ' ', ' '.join(
    #             section.xpath(
    #                 './/span[contains(text(), "Status:")]/following-sibling::strong//text() | .//span[contains(text(), "Status:")]/following-sibling::b//text()').getall())).strip()
    #         part["Condition"] = re.sub('\s+', ' ', ' '.join(
    #             section.xpath(
    #                 './/span[contains(text(), "Condition:")]/following-sibling::strong//text() | .//span[contains(text(), "Condition:")]/following-sibling::b//text()').getall())).strip()
    #         part["Mileage"] = re.sub('\s+', ' ', ' '.join(
    #             section.xpath(
    #                 './/span[contains(text(), "Mileage:")]/following-sibling::strong//text() | .//span[contains(text(), "Mileage:")]/following-sibling::b//text()').getall())).strip()
    #         yield part

    #     next_page_href = response.selector.xpath(f'.//a[contains(@href, "currentpage={currentpage + 1}")]/@href').get()
    #     if next_page_href and len(next_page_href) > 0:
    #         yield scrapy.Request(
    #             url=next_page_href,
    #             callback=self.parse_listing_page,
    #             headers=self.headers,
    #             meta={"filter": car_data,
    #                   "currentpage": currentpage + 1})
