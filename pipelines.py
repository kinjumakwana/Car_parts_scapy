# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface

import mysql.connector

class StoreDataToMySQL:

    def __init__(self):
        self.connection = None
        self.cursor = None
        self.table_name = 'kosiskiautopart'
        self.url_table_name = 'url_table'
        self.create_connection()
        self.create_table()
        pass

    def create_connection(self):
        self.connection = mysql.connector.connect(
            host='localhost',
            # user='root',
            port='3306',
            user='yoewalle_a2wp169',
            password='yoewalle_a2wp169_2020',
            # password='k4t13*D48#gS8w3*D48#g',
            database='kosiskiautopartsearch',
        )
        self.cursor = self.connection.cursor()

    def create_table(self):
        self.cursor.execute(f"""CREATE TABLE IF NOT EXISTS {self.table_name}(
                    Car_Name VARCHAR(255),
                    Car_Year VARCHAR(255),
                    Car_Model VARCHAR(255),
                    Car_Part VARCHAR(255),
                    Part_ID VARCHAR(255),
                    Part_href VARCHAR(500), 
                    Price VARCHAR(255), 
                    Yard_Notes VARCHAR(255), 
                    Application VARCHAR(255),
                    Body_Color VARCHAR(255), 
                    Vehicle VARCHAR(255), 
                    Warehouse_ID VARCHAR(255), 
                    Year VARCHAR(255),
                    Status VARCHAR(255), 
                    `Condition` VARCHAR(255),
                    Mileage VARCHAR(255), 
                    UNIQUE KEY(Part_href)

        )""")
        
        # Create the URL table for storing URLs
        self.cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {self.url_table_name} (
                id INT AUTO_INCREMENT PRIMARY KEY,
                url TEXT,
                maker VARCHAR(255),
                year INT,
                model VARCHAR(255),
                part VARCHAR(255),
                currentpage INT, 
                UNIQUE (url(255))
                
            )
        ''')

    def store_db(self, item):
        command = (f"INSERT INTO {self.table_name} "
                   f"(Car_Name, Car_Year, Car_Model, Car_Part, Part_ID, Part_href, Price, Yard_Notes, Application, Body_Color, Vehicle, Warehouse_ID, Year, Status, `Condition`, Mileage) "
                   f"VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s , %s, %s, %s, %s, %s, %s) "
                   f"ON DUPLICATE KEY UPDATE "
                   f"Car_Name = VALUES(Car_Name), "
                   f"Car_Year = VALUES(Car_Year), "
                   f"Car_Model = VALUES(Car_Model), "
                   f"Car_Part = VALUES(Car_Part), "
                   f"Part_ID = VALUES(Part_ID), "
                   f"Price = VALUES(Price), "
                   f"Yard_Notes = VALUES(Yard_Notes), "
                   f"Application = VALUES(Application), "
                   f"Body_Color = VALUES(Body_Color), "
                   f"Vehicle = VALUES(Vehicle), "
                   f"Warehouse_ID = VALUES(Warehouse_ID), "
                   f"Year = VALUES(Year), "
                   f"Status = VALUES(Status), "
                   f"`Condition` = VALUES(`Condition`), "
                   f"Mileage = VALUES(Mileage)")

        data = (
            item['Car_Name'],
            item['Car_Year'],
            item['Car_Model'],
            item['Car_Part'],
            item['Part_ID'],
            item['Part_href'],
            item['Price'],
            item['Yard_Notes'],
            item['Application'],
            item['Body_Color'],
            item['Vehicle'],
            item['Warehouse_ID'],
            item['Year'],
            item['Status'],
            item['Condition'],
            item['Mileage'])

        self.cursor.execute(command, data)
        self.connection.commit()

    def store_url(self, url, maker, year, model, part, currentpage):
        print("Storing URL:", url)
        query = f"INSERT INTO {self.url_table_name} (url, maker, year, model, part, currentpage) VALUES (%s, %s, %s, %s, %s, %s)"
        f"ON DUPLICATE KEY UPDATE "
        self.cursor.execute(query, (url, maker, year, model, part, currentpage))
        self.connection.commit()
        print("URL Stored!")
        self.logger.info(f"Stored URL: {url}")
     
    def process_item(self, item, spider):
        if 'url' in item:
            url = item['url']
            maker = item['maker']
            year = item['year']
            model = item['model']
            part = item['part']
            currentpage = item['currentpage']
            print("Processing item with URL:", url)
            try:
                self.store_url(url, maker, year, model, part, currentpage)
                print("Data inserted into url_table successfully.")
            except Exception as e:
                print("Error inserting data into url_table:", e)
        
        try:
            self.store_db(item)
            print("Data inserted into main data table successfully.")
        except Exception as e:
            print("Error inserting data into main data table:", e)
        return item

    def close_spider(self, spider):
        self.connection.close()
        self.cursor.close()
        pass
